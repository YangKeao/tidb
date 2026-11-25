"""Match native SQL registrations with the signatures supported by the SDK."""

import collections
import json
import re
import sys
from pathlib import Path


def go_source(path):
    tokens = re.compile(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|`[^`]*`', re.S)
    return tokens.sub(
        lambda m: "\n" * m[0].count("\n") if m[0].startswith(("//", "/*")) else m[0],
        path.read_text(),
    )


def native_registrations(tidb):
    ast = go_source(tidb / "pkg/parser/ast/functions.go")
    names = dict(re.findall(r'^\s*(\w+)\s*=\s*"([^"]+)"', ast, re.M))
    builtin = go_source(tidb / "pkg/expression/builtin.go")
    constructors = re.findall(
        r'^\s*ast\.(\w+):\s*&(\w+FunctionClass)\{baseFunctionClass\{ast\.\w+,'
        r'\s*(-?\d+),\s*(-?\d+)\}(.*?)\},',
        builtin,
        re.M,
    )
    registrations = collections.defaultdict(list)
    for symbol, klass, minimum, maximum, config in constructors:
        if symbol in names:
            registrations[klass].append(
                (names[symbol], int(minimum), int(maximum), config.strip().lstrip(",").strip())
            )

    # CAST is built through its constructor helper rather than the SQL map.
    cast = go_source(tidb / "pkg/expression/builtin_cast.go")
    helper = re.search(r'^func BuildCastFunctionWithCheck\(.*?^}', cast, re.M | re.S)[0]
    for klass, minimum, maximum in re.findall(
        r'&(\w+FunctionClass)\{baseFunctionClass\{ast\.Cast,\s*(\d+),\s*(\d+)\}',
        helper,
    ):
        registrations[klass].append((names["Cast"], int(minimum), int(maximum), ""))
    # Internal helpers construct classes directly using a string constant.
    for path in sorted((tidb / "pkg/expression").glob("builtin*.go")):
        if path.name.endswith("_test.go"):
            continue
        source = go_source(path)
        internal = dict(re.findall(r'^const (InternalFunc\w+) = "([^"]+)"', source, re.M))
        for klass, symbol, minimum, maximum in re.findall(
            r'&(\w+FunctionClass)\{baseFunctionClass\{(InternalFunc\w+), (\d+), (-?\d+)\}',
            source,
        ):
            if symbol in internal:
                registrations[klass].append(
                    (internal[symbol], int(minimum), int(maximum), "")
                )
    return registrations


def native_methods(tidb):
    fields = {}
    methods = collections.defaultdict(list)
    callbacks = {}
    for path in sorted((tidb / "pkg/expression").glob("builtin*.go")):
        if path.name.endswith("_test.go"):
            continue
        source = go_source(path)
        for match in re.finditer(
            r'^func (\w+)\(([^\n]*)\) \{\s+\w+\.setPbCode\((\w+)\)\s+\}',
            source, re.M,
        ):
            parameters = [field.strip().split()[0] for field in match[2].split(",")]
            if match[3] in parameters:
                callbacks[match[1]] = parameters.index(match[3])
        for match in re.finditer(r'^type (\w+FunctionClass) struct \{(.*?)^}', source, re.M | re.S):
            fields[match[1]] = re.findall(r'^\s*([A-Za-z_]\w*)\s+(\w+(?:\.\w+)?)\s*$', match[2], re.M)
        for match in re.finditer(r'^func \((\w+) \*(\w+FunctionClass)\).*?^}', source, re.M | re.S):
            methods[match[2]].append((match[1], match[0]))
    return fields, methods, callbacks


def references(receiver, method, config, callbacks):
    """Follow switches and boolean branches on fixed constructor fields."""
    active = {}
    switches = {}
    result = set()
    prefix = re.escape(receiver) + r'\.'
    for line in method.splitlines():
        level = len(line) - len(line.lstrip("\t"))
        line = line.strip()
        if line == "} else {" and level in active and level not in switches:
            active[level] = not active[level]
        elif line.startswith("}"):
            active.pop(level, None)
            switches.pop(level, None)
        elif match := re.fullmatch(r'if (!?)' + prefix + r'(\w+) \{', line):
            if match[2] in config and config[match[2]] in ("true", "false"):
                active[level] = (config[match[2]] == "true") != bool(match[1])
        elif match := re.fullmatch(r'switch ' + prefix + r'(\w+) \{', line):
            if match[1] in config:
                switches[level] = config[match[1]]
        elif level in switches and line.startswith("case "):
            active[level] = switches[level] in [value.strip() for value in line[5:-1].split(",")]
        if all(active.values()):
            if config and (dispatch := re.fullmatch(prefix + r'(\w+)\((.*)\)', line)):
                if dispatch[1] in config and "tipb.ScalarFuncSig_" in dispatch[2]:
                    selected = callbacks.get(config[dispatch[1]])
                    arguments = [arg.strip() for arg in dispatch[2].split(",")]
                    if selected is not None and selected < len(arguments):
                        result.update(re.findall(r'tipb\.ScalarFuncSig_(\w+)', arguments[selected]))
                    continue
            result.update(re.findall(r'tipb\.ScalarFuncSig_(\w+)', line))
    return result


def unexported_signature(method):
    """Resolve a single returned builtin whose class declares no PB code."""
    if "tipb.ScalarFuncSig_" in method:
        return None
    constructors = re.findall(r'^\s*(\w+) := &builtin(\w+)Sig\{', method, re.M)
    if len(constructors) == 1:
        value, signature = constructors[0]
        if re.search(r'^\s*return ' + re.escape(value) + r', nil$', method, re.M):
            return signature
    return None


def sdk_signatures(tikv):
    source = (tikv / "components/tidb_query_expr/src/lib.rs").read_text()
    entry = re.search(r"^\s*(?:pub )?fn (?:resolve_rpn_function|map_expr_node_to_rpn_func)\(", source, re.M)
    if entry is None:
        raise ValueError(f"cannot find the RPN mapper in {tikv}")
    registry = source[entry.end():].split("fn map_expr_node_to_sc_func", 1)[0]
    return list(dict.fromkeys(re.findall(r'ScalarFuncSig::(\w+)', registry)))


def main(tidb, tikv):
    registrations = native_registrations(tidb)
    field_types, methods, callbacks = native_methods(tidb)
    classes = collections.defaultdict(set)
    bindings = collections.defaultdict(set)
    for klass, method_list in methods.items():
        for receiver, method in method_list:
            local = unexported_signature(method)
            for sig in references(receiver, method, {}, callbacks):
                classes[sig].add(klass)
            if local:
                classes[local].add(klass)
            for name, minimum, maximum, configuration in registrations[klass]:
                values = configuration.split(", ") if configuration else []
                dimensions = field_types.get(klass, [])
                if len({entry[3] for entry in registrations[klass]}) > 1:
                    if len(values) != len(dimensions) or any(
                        kind != "bool" and not re.fullmatch(r'\w+(?:\.\w+)?', value)
                        for (_, kind), value in zip(dimensions, values)
                    ):
                        continue
                config = dict(zip((field for field, _ in dimensions), values))
                for sig in references(receiver, method, config, callbacks):
                    bindings[sig].add((name, minimum, maximum))
                if local:
                    bindings[local].add((name, minimum, maximum))

    supported = sdk_signatures(tikv)
    def canonical_index(index):
        canonical = collections.defaultdict(set)
        for spelling, values in index.items():
            canonical[spelling.casefold()].update(values)
        return canonical

    # Go and Rust protobuf generators use different acronym capitalization.
    classes = canonical_index(classes)
    bindings = canonical_index(bindings)
    # Go's JSON IS NULL casts to text and emits StringIsNull. The SDK can
    # inspect the existing typed JSON carrier without converting any value.
    bindings["jsonisnull"] = bindings["stringisnull"].copy()
    inventory = [
        dict(signature=sig, native_classes=sorted(classes[sig.casefold()]), candidates=sorted(bindings[sig.casefold()]))
        for sig in supported
    ]
    counts = collections.Counter(
        "source_binding" if record["candidates"] else "requires_binding_audit"
        for record in inventory
    )
    print(json.dumps({"overloads": len(inventory), "counts": counts, "signatures": inventory}, indent=2))


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
