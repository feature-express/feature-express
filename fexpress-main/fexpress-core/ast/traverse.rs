use std::collections::VecDeque;

use itertools::Itertools;

use crate::ast::core::{Expr, ExprFunc};

pub trait ExprVisitor {
    // Define a method for each variant, or just one method if you prefer
    fn visit(&mut self, expr: &mut Expr);
}

impl Expr {
    pub fn get_expr(&self) -> Vec<Expr> {
        match self {
            Expr::EventType => vec![],
            Expr::EventTime => vec![],
            Expr::EventId => vec![],
            Expr::ObservationDate => vec![],
            Expr::EntityId(_) => vec![],
            Expr::AttrBool(_) => vec![],
            Expr::AttrNum(_) => vec![],
            Expr::AttrInt(_) => vec![],
            Expr::AttrStr(_) => vec![],
            Expr::AttrMapNum(_) => vec![],
            Expr::AttrMapStr(_) => vec![],
            Expr::AttrVecStr(_) => vec![],
            Expr::AttrVecInt(_) => vec![],
            Expr::AttrVecNum(_) => vec![],
            Expr::AttrVecBool(_) => vec![],
            Expr::AttrDate(_) => vec![],
            Expr::AttrDateTime(_) => vec![],
            Expr::AttrUntyped(_) => vec![],
            Expr::None => vec![],
            Expr::Wildcard => vec![],
            Expr::LitBool(_) => vec![],
            Expr::LitNum(_) => vec![],
            Expr::LitInt(_) => vec![],
            Expr::LitStr(_) => vec![],
            Expr::LitDate(_) => vec![],
            Expr::LitDateTime(_) => vec![],
            Expr::TupleLitBool(_) => vec![],
            Expr::TupleLitNum(_) => vec![],
            Expr::TupleLitInt(_) => vec![],
            Expr::TupleLitStr(_) => vec![],
            Expr::Add(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Sub(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Mul(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Div(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Eq(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Neq(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::GreaterEq(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::LessEq(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Greater(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Less(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::And(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Or(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Not(e1) => vec![*e1.clone()],
            Expr::In(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::NotIn(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Function(f) => match f {
                ExprFunc::Floor(e1) => vec![*e1.clone()],
                ExprFunc::Ceil(e1) => vec![*e1.clone()],
                ExprFunc::Round(e1) => vec![*e1.clone()],
                ExprFunc::Trunc(e1) => vec![*e1.clone()],
                ExprFunc::Fract(e1) => vec![*e1.clone()],
                ExprFunc::Abs(e1) => vec![*e1.clone()],
                ExprFunc::Signum(e1) => vec![*e1.clone()],
                ExprFunc::DivEuclid(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::RemEuclid(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Powf(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Sqrt(e1) => vec![*e1.clone()],
                ExprFunc::Exp(e1) => vec![*e1.clone()],
                ExprFunc::Exp2(e1) => vec![*e1.clone()],
                ExprFunc::Ln(e1) => vec![*e1.clone()],
                ExprFunc::Log(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Log2(e1) => vec![*e1.clone()],
                ExprFunc::Log10(e1) => vec![*e1.clone()],
                ExprFunc::Sin(e1) => vec![*e1.clone()],
                ExprFunc::Cos(e1) => vec![*e1.clone()],
                ExprFunc::Tan(e1) => vec![*e1.clone()],
                ExprFunc::Asin(e1) => vec![*e1.clone()],
                ExprFunc::Acos(e1) => vec![*e1.clone()],
                ExprFunc::Atan(e1) => vec![*e1.clone()],
                ExprFunc::Expm1(e1) => vec![*e1.clone()],
                ExprFunc::Ln1p(e1) => vec![*e1.clone()],
                ExprFunc::Sinh(e1) => vec![*e1.clone()],
                ExprFunc::Cosh(e1) => vec![*e1.clone()],
                ExprFunc::Asinh(e1) => vec![*e1.clone()],
                ExprFunc::Acosh(e1) => vec![*e1.clone()],
                ExprFunc::Atanh(e1) => vec![*e1.clone()],
                ExprFunc::Len(e1) => vec![*e1.clone()],
                ExprFunc::Substr(e1, e2, e3) => vec![*e1.clone(), *e2.clone(), *e3.clone()],
                ExprFunc::Concat(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Trim(e1) => vec![*e1.clone()],
                ExprFunc::Lower(e1) => vec![*e1.clone()],
                ExprFunc::Upper(e1) => vec![*e1.clone()],
                ExprFunc::Replace(e1, e2, e3) => vec![*e1.clone(), *e2.clone(), *e3.clone()],
                ExprFunc::StartsWith(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::EndsWith(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Contains(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::RegexMatch(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::RegexExtract(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::RegexReplace(e1, e2, e3) => vec![*e1.clone(), *e2.clone(), *e3.clone()],
                ExprFunc::RegexSplit(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::RegexCount(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Coalesce(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::DateAdd(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::DateSub(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Hour(e1) => vec![*e1.clone()],
                ExprFunc::Minute(e1) => vec![*e1.clone()],
                ExprFunc::Second(e1) => vec![*e1.clone()],
                ExprFunc::Microsecond(e1) => vec![*e1.clone()],
                ExprFunc::DatePart(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Extract(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::FormatDate(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Now => vec![],
                ExprFunc::CurrentDate => vec![],
                ExprFunc::CurrentTime => vec![],
                ExprFunc::Date(e1) => vec![*e1.clone()],
                ExprFunc::DateDiff(e1, e2) => vec![*e1.clone(), *e2.clone()],
                ExprFunc::Year(e1) => vec![*e1.clone()],
                ExprFunc::Month(e1) => vec![*e1.clone()],
                ExprFunc::Day(e1) => vec![*e1.clone()],
                ExprFunc::Week(e1) => vec![*e1.clone()],
                ExprFunc::Weekday(e1) => vec![*e1.clone()],
                ExprFunc::DayOfYear(e1) => vec![*e1.clone()],
                ExprFunc::Quarter(e1) => vec![*e1.clone()],
                ExprFunc::IsStartOfMonth(e1) => vec![*e1.clone()],
                ExprFunc::IsEndOfMonth(e1) => vec![*e1.clone()],
                ExprFunc::IsWeekend(e1) => vec![*e1.clone()],
                ExprFunc::If(e1, e2, e3) => vec![*e1.clone(), *e2.clone(), *e3.clone()],
                ExprFunc::Clamp(e1, e2, e3) => vec![*e1.clone(), *e2.clone(), *e3.clone()],
            },
            Expr::ParsingError(_e1) => vec![],
            Expr::FullQuery(fq) => fq.select_exprs.clone().into_iter().collect_vec(),
            Expr::ContextAttr(_e1) => vec![],
            Expr::Cons(e1, e2) => vec![*e1.clone(), *e2.clone()],
            Expr::Aggr(aggr) => {
                let mut v = vec![];
                if let Some(expr) = &aggr.cond {
                    v.push(*(*expr).clone());
                }
                if let Some(expr) = &aggr.groupby {
                    v.push(*(*expr).clone());
                }
                if let Some(expr) = &aggr.having {
                    v.push(*(expr.expr).clone());
                }
                v.push(*(aggr.agg_expr).clone());
                v
            }
            Expr::Having(having) => vec![*having.expr.clone()],
            Expr::Alias(_, e) => vec![*e.clone()],
            Expr::VariableAssign(_, e) => vec![*e.clone()],
            Expr::Select(select) => select.clone().expressions,
        }
    }

    pub fn visit(&mut self, visitor: &mut impl ExprVisitor) {
        // Visit children first
        match self {
            Expr::Cons(left, right)
            | Expr::Add(left, right)
            | Expr::Sub(left, right)
            | Expr::Mul(left, right)
            | Expr::Div(left, right)
            | Expr::Eq(left, right)
            | Expr::Neq(left, right)
            | Expr::GreaterEq(left, right)
            | Expr::LessEq(left, right)
            | Expr::Greater(left, right)
            | Expr::Less(left, right)
            | Expr::And(left, right)
            | Expr::Or(left, right)
            | Expr::In(left, right)
            | Expr::NotIn(left, right) => {
                left.visit(visitor);
                right.visit(visitor);
            }
            Expr::Not(expr) | Expr::Alias(_, expr) | Expr::VariableAssign(_, expr) => {
                expr.visit(visitor);
            }
            Expr::Aggr(aggr) => {
                if let Some(expr) = &mut aggr.cond {
                    expr.visit(visitor);
                }
                if let Some(expr) = &mut aggr.groupby {
                    expr.visit(visitor);
                }
                if let Some(expr) = &mut aggr.having {
                    expr.expr.visit(visitor);
                }
                aggr.agg_expr.visit(visitor);
            }
            Expr::Having(having) => having.expr.visit(visitor),
            Expr::Select(select) => {
                for expr in select.expressions.iter_mut() {
                    expr.visit(visitor);
                }
            }
            Expr::FullQuery(query) => {
                for expr in query.select_exprs.iter_mut() {
                    expr.visit(visitor)
                }
            }
            Expr::EventType => {}
            Expr::EventTime => {}
            Expr::EventId => {}
            Expr::ObservationDate => {}
            Expr::EntityId(_) => {}
            Expr::AttrBool(_) => {}
            Expr::AttrNum(_) => {}
            Expr::AttrInt(_) => {}
            Expr::AttrStr(_) => {}
            Expr::AttrMapNum(_) => {}
            Expr::AttrMapStr(_) => {}
            Expr::AttrVecStr(_) => {}
            Expr::AttrVecInt(_) => {}
            Expr::AttrVecNum(_) => {}
            Expr::AttrVecBool(_) => {}
            Expr::AttrDate(_) => {}
            Expr::AttrDateTime(_) => {}
            Expr::AttrUntyped(_) => {}
            Expr::ContextAttr(_) => {}
            Expr::None => {}
            Expr::Wildcard => {}
            Expr::LitBool(_) => {}
            Expr::LitNum(_) => {}
            Expr::LitInt(_) => {}
            Expr::LitStr(_) => {}
            Expr::LitDate(_) => {}
            Expr::LitDateTime(_) => {}
            Expr::TupleLitBool(_) => {}
            Expr::TupleLitNum(_) => {}
            Expr::TupleLitInt(_) => {}
            Expr::TupleLitStr(_) => {}
            Expr::Function(f) => match f {
                ExprFunc::Floor(e1) => e1.visit(visitor),
                ExprFunc::Ceil(e1) => e1.visit(visitor),
                ExprFunc::Round(e1) => e1.visit(visitor),
                ExprFunc::Trunc(e1) => e1.visit(visitor),
                ExprFunc::Fract(e1) => e1.visit(visitor),
                ExprFunc::Abs(e1) => e1.visit(visitor),
                ExprFunc::Signum(e1) => e1.visit(visitor),
                ExprFunc::DivEuclid(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::RemEuclid(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Powf(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Sqrt(e1) => e1.visit(visitor),
                ExprFunc::Exp(e1) => e1.visit(visitor),
                ExprFunc::Exp2(e1) => e1.visit(visitor),
                ExprFunc::Ln(e1) => e1.visit(visitor),
                ExprFunc::Log(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Log2(e1) => e1.visit(visitor),
                ExprFunc::Log10(e1) => e1.visit(visitor),
                ExprFunc::Sin(e1) => e1.visit(visitor),
                ExprFunc::Cos(e1) => e1.visit(visitor),
                ExprFunc::Tan(e1) => e1.visit(visitor),
                ExprFunc::Asin(e1) => e1.visit(visitor),
                ExprFunc::Acos(e1) => e1.visit(visitor),
                ExprFunc::Atan(e1) => e1.visit(visitor),
                ExprFunc::Expm1(e1) => e1.visit(visitor),
                ExprFunc::Ln1p(e1) => e1.visit(visitor),
                ExprFunc::Sinh(e1) => e1.visit(visitor),
                ExprFunc::Cosh(e1) => e1.visit(visitor),
                ExprFunc::Asinh(e1) => e1.visit(visitor),
                ExprFunc::Acosh(e1) => e1.visit(visitor),
                ExprFunc::Atanh(e1) => e1.visit(visitor),
                ExprFunc::Len(e1) => e1.visit(visitor),
                ExprFunc::Substr(e1, e2, e3) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                    e3.visit(visitor);
                }
                ExprFunc::Concat(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Trim(e1) => e1.visit(visitor),
                ExprFunc::Lower(e1) => e1.visit(visitor),
                ExprFunc::Upper(e1) => e1.visit(visitor),
                ExprFunc::Replace(e1, e2, e3) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                    e3.visit(visitor);
                }
                ExprFunc::StartsWith(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::EndsWith(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Contains(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::RegexMatch(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::RegexExtract(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::RegexReplace(e1, e2, e3) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                    e3.visit(visitor);
                }
                ExprFunc::RegexSplit(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::RegexCount(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Coalesce(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::DateAdd(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::DateSub(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Hour(e1) => e1.visit(visitor),
                ExprFunc::Minute(e1) => e1.visit(visitor),
                ExprFunc::Second(e1) => e1.visit(visitor),
                ExprFunc::Microsecond(e1) => e1.visit(visitor),
                ExprFunc::DatePart(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Extract(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::FormatDate(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Now => {}
                ExprFunc::CurrentDate => {}
                ExprFunc::CurrentTime => {}
                ExprFunc::Date(e1) => e1.visit(visitor),
                ExprFunc::DateDiff(e1, e2) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                }
                ExprFunc::Year(e1) => e1.visit(visitor),
                ExprFunc::Month(e1) => e1.visit(visitor),
                ExprFunc::Day(e1) => e1.visit(visitor),
                ExprFunc::Week(e1) => e1.visit(visitor),
                ExprFunc::Weekday(e1) => e1.visit(visitor),
                ExprFunc::DayOfYear(e1) => e1.visit(visitor),
                ExprFunc::Quarter(e1) => e1.visit(visitor),
                ExprFunc::IsStartOfMonth(e1) => e1.visit(visitor),
                ExprFunc::IsEndOfMonth(e1) => e1.visit(visitor),
                ExprFunc::IsWeekend(e1) => e1.visit(visitor),
                ExprFunc::If(e1, e2, e3) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                    e3.visit(visitor);
                }
                ExprFunc::Clamp(e1, e2, e3) => {
                    e1.visit(visitor);
                    e2.visit(visitor);
                    e3.visit(visitor);
                }
            },
            Expr::ParsingError(_) => {}
        }

        // Visit the current node
        visitor.visit(self);
    }

    pub fn recursive_expr(&self) -> Vec<Expr> {
        let mut results = vec![self.clone()]; // Include the root

        for expr in self.get_expr() {
            results.extend(expr.recursive_expr());
        }

        results
    }
}

pub fn traverse_expr<T>(expr: &Expr, callback: &dyn Fn(&Expr) -> Option<T>) -> Vec<T> {
    let mut results = Vec::new();
    let mut queue = VecDeque::new();
    queue.push_back(expr.clone());

    while let Some(current_expr) = queue.pop_front() {
        if let Some(result) = callback(&current_expr) {
            results.push(result);
        }

        for child_expr in current_expr.get_expr() {
            queue.push_back(child_expr.clone());
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::ast::traverse::traverse_expr;
    use crate::event::AttributeKey;

    use super::*;

    #[test]
    fn test_traverse_expr() {
        // Construct a sample expression tree
        let expr = Expr::Add(
            Box::new(Expr::Add(
                Box::new(Expr::LitInt(1)),
                Box::new(Expr::LitInt(2)),
            )),
            Box::new(Expr::LitInt(3)),
        );

        // Define the callback
        let callback = |expr: &Expr| match expr {
            Expr::Add(_, _) => Some(expr.clone()),
            _ => None,
        };

        // Call the function
        let results = traverse_expr(&expr, &callback);

        // Assertions
        assert_eq!(results.len(), 2); // There are two `Expr::Add` nodes
        for result in results {
            if let Expr::Add(_, _) = result {
                // This is expected
            } else {
                panic!("Unexpected expression type");
            }
        }
    }

    struct TypedAttributeVisitor;

    fn get_typed_attribute(key: &AttributeKey) -> Expr {
        // Logic to determine the typed attribute variant...
        // Return the appropriate typed variant, e.g., Expr::AttrInt(*key)
        Expr::AttrInt(key.clone())
    }

    impl ExprVisitor for TypedAttributeVisitor {
        fn visit(&mut self, expr: &mut Expr) {
            if let Expr::AttrUntyped(key) = expr {
                *expr = get_typed_attribute(key);
            }
        }
    }

    #[test]
    fn test_typed_attribute_visitor() {
        let mut expr = Expr::AttrUntyped(AttributeKey::from_str("some_key").unwrap());
        let mut visitor = TypedAttributeVisitor;
        expr.visit(&mut visitor);
        let expected_expr = Expr::AttrInt(AttributeKey::from_str("some_key").unwrap());
        assert_eq!(expr, expected_expr);
    }
}
