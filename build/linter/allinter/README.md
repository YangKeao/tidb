# Alllinter

Execute the following command to run all defined CI in tidb repo:

```sh
go run ./build/linter/allinter/main.go ./...
```

It'll use a lot of memory, so be careful about the OOM :)