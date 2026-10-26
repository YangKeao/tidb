// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Borrowed metadata views retain the actual caller root and its identity.
use super::*;
#[derive(Clone, Copy)]
pub(super) enum SourceNodeRef<'a> {
    Expression(&'a Expression),
    Function(&'a ScalarFunction),
}
#[derive(Clone, Copy)]
pub(super) enum BorrowedNodeKind<'a> {
    Constant(&'a crate::constant::Constant),
    Column(&'a crate::column::Column),
    CorrelatedColumn(&'a crate::column::CorrelatedColumn),
    ScalarFunction(&'a ScalarFunction),
}
impl<'a> From<&'a Expression> for SourceNodeRef<'a> {
    fn from(value: &'a Expression) -> Self {
        Self::Expression(value)
    }
}
impl<'a> From<&'a ScalarFunction> for SourceNodeRef<'a> {
    fn from(value: &'a ScalarFunction) -> Self {
        Self::Function(value)
    }
}
impl<'a> SourceNodeRef<'a> {
    pub(super) fn kind(self) -> BorrowedNodeKind<'a> {
        match self {
            Self::Function(function) => BorrowedNodeKind::ScalarFunction(function),
            Self::Expression(node) => match node {
                Expression::Constant(value) => BorrowedNodeKind::Constant(value),
                Expression::Column(value) => BorrowedNodeKind::Column(value),
                Expression::CorrelatedColumn(value) => BorrowedNodeKind::CorrelatedColumn(value),
                Expression::ScalarFunction(value) => BorrowedNodeKind::ScalarFunction(value),
            },
        }
    }
    pub(super) fn expression(self) -> Option<&'a Expression> {
        match self {
            Self::Expression(value) => Some(value),
            Self::Function(_) => None,
        }
    }
    pub(super) fn identity(self) -> usize {
        match self {
            Self::Expression(value) => std::ptr::from_ref(value) as usize,
            Self::Function(value) => std::ptr::from_ref(value) as usize,
        }
    }
    pub(super) fn function_root(self) -> bool {
        matches!(self, Self::Function(_))
    }
    pub(super) fn default_view(self, limits: CompileLimits) -> NativeView {
        match self {
            Self::Expression(value) => root_source_view(value, limits),
            Self::Function(_) => NativeView::FunctionRoot,
        }
    }
    pub(super) fn folds_to_constant(self) -> bool {
        match self {
            Self::Expression(value) => crate::constant_fold::folds_to_constant(value),
            Self::Function(value) => crate::constant_fold::function_folds_to_constant(value),
        }
    }
    pub(super) fn math_bound(
        self,
        children: Option<usize>,
    ) -> std::result::Result<Option<usize>, &'static str> {
        match self {
            Self::Expression(value) => {
                crate::scalar_function::math_diagnostic_node_bound(value, children)
            }
            Self::Function(value) => {
                crate::scalar_function::math_diagnostic_function_bound(value, children)
            }
        }
    }
    pub(super) fn numeric_bound(
        self,
        children: Option<usize>,
    ) -> std::result::Result<Option<usize>, &'static str> {
        match self {
            Self::Expression(value) => {
                crate::scalar_function::numeric_diagnostic_node_bound(value, children)
            }
            Self::Function(value) => {
                crate::scalar_function::numeric_diagnostic_function_bound(value, children)
            }
        }
    }
    pub(super) fn numeric_failure(self, children: Option<[u8; 8]>) -> Option<[u8; 8]> {
        match self {
            Self::Expression(value) => {
                crate::scalar_function::numeric_diagnostic_format_failure(value, children)
            }
            Self::Function(value) => {
                crate::scalar_function::numeric_diagnostic_function_format_failure(value, children)
            }
        }
    }
}

/// Keep the existing one-pointer storage for Expression-only consumers. The
/// genuine direct-function consumer uses the wider sum reference and pays its
/// actual size; both instantiate the same source compiler and demand policy.
pub(super) trait SourceReference<'a>:
    Copy + Into<SourceNodeRef<'a>> + From<&'a Expression>
{
    fn kind(self) -> BorrowedNodeKind<'a> {
        self.into().kind()
    }
    fn expression(self) -> Option<&'a Expression> {
        self.into().expression()
    }
    fn identity(self) -> usize {
        self.into().identity()
    }
    fn function_root(self) -> bool {
        self.into().function_root()
    }
    fn default_view(self, limits: CompileLimits) -> NativeView {
        self.into().default_view(limits)
    }
    fn folds_to_constant(self) -> bool {
        self.into().folds_to_constant()
    }
    fn math_bound(
        self,
        children: Option<usize>,
    ) -> std::result::Result<Option<usize>, &'static str> {
        self.into().math_bound(children)
    }
    fn numeric_bound(
        self,
        children: Option<usize>,
    ) -> std::result::Result<Option<usize>, &'static str> {
        self.into().numeric_bound(children)
    }
    fn numeric_failure(self, children: Option<[u8; 8]>) -> Option<[u8; 8]> {
        self.into().numeric_failure(children)
    }
}
impl<'a> SourceReference<'a> for &'a Expression {}
impl<'a> SourceReference<'a> for SourceNodeRef<'a> {}
