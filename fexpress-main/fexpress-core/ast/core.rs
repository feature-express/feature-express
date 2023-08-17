use std::fmt::{Display, Formatter};

use crate::map::HashSet;
use crate::sstring::SmallString;

use chrono::{NaiveDate, NaiveDateTime};
use itertools::Itertools;
use ordered_float::OrderedFloat;

use strum::{EnumDiscriminants, EnumIter};

use crate::event::{AttributeKey, EntityType};
use crate::features::Feature;
use crate::interval::NewInterval;

use crate::types::{FLOAT, INT};

use enum_as_inner::EnumAsInner;

pub type BExpr = Box<Expr>;

use crate::ast::analyze::CalculationNode;
use crate::ast::traverse::traverse_expr;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

#[derive(Clone, Debug, Eq, PartialEq, Hash, EnumAsInner)]
pub enum Expr {
    FullQuery(FullQuery),

    // features
    EventType,
    EventTime,
    EventId,
    ObservationDate,
    EntityId(EntityType),

    // probably all the attributes apart from the untyped can be converted to casting operators
    AttrBool(AttributeKey),
    AttrNum(AttributeKey),
    AttrInt(AttributeKey),
    AttrStr(AttributeKey),
    AttrMapNum(AttributeKey),
    AttrMapStr(AttributeKey),
    AttrVecStr(AttributeKey),
    AttrVecInt(AttributeKey),
    AttrVecNum(AttributeKey),
    AttrVecBool(AttributeKey),
    AttrDate(AttributeKey),
    AttrDateTime(AttributeKey),
    AttrUntyped(AttributeKey),
    // attribute or entity refers to the current
    ContextAttr(AttributeKey),

    // literal values
    None,
    Wildcard,
    LitBool(bool),
    LitNum(OrderedFloat<FLOAT>),
    LitInt(INT),
    LitStr(String),
    LitDate(NaiveDate),
    LitDateTime(NaiveDateTime),

    // tuples
    TupleLitBool(Vec<bool>),
    TupleLitNum(Vec<OrderedFloat<FLOAT>>),
    TupleLitInt(Vec<INT>),
    TupleLitStr(Vec<String>),

    Cons(BExpr, BExpr),

    // math operations
    Add(BExpr, BExpr),
    Sub(BExpr, BExpr),
    Mul(BExpr, BExpr),
    Div(BExpr, BExpr),

    // relations
    Eq(BExpr, BExpr),
    Neq(BExpr, BExpr),
    GreaterEq(BExpr, BExpr),
    LessEq(BExpr, BExpr),
    Greater(BExpr, BExpr),
    Less(BExpr, BExpr),

    // logical
    And(BExpr, BExpr),
    Or(BExpr, BExpr),
    Not(BExpr),

    // in / not in
    In(BExpr, BExpr),
    NotIn(BExpr, BExpr),

    // aggregation
    Aggr(AggrExpr),
    Having(HavingExpr),
    Alias(SmallString, BExpr),
    VariableAssign(SmallString, BExpr),

    // full query
    Select(SelectExpr),

    Function(ExprFunc),

    // error
    ParsingError(String),
}

impl Expr {
    pub fn extract_alias(expr: &Expr) -> Option<Expr> {
        match expr {
            Expr::Alias(_, _) => Some(expr.clone()),
            _ => None,
        }
    }

    pub fn extract_aliases(&self) -> Vec<Expr> {
        traverse_expr(self, &Self::extract_alias)
    }

    pub fn extract_aggregation(expr: &Expr) -> Option<Expr> {
        match expr {
            Expr::Aggr(_) => Some(expr.clone()),
            _ => None,
        }
    }

    pub fn extract_aggregations(&self) -> Vec<Expr> {
        traverse_expr(self, &Self::extract_aggregation)
    }

    pub fn extract_attribute(expr: &Expr) -> Option<Expr> {
        match expr {
            Expr::AttrUntyped(_) => Some(expr.clone()),
            _ => None,
        }
    }

    pub fn extract_attributes(&self) -> Vec<Expr> {
        traverse_expr(self, &Self::extract_attribute)
    }

    /*
    This returns all the edges in the aggregation
     */
    pub fn extract_calculation_node_edges(&self) -> Vec<(CalculationNode, CalculationNode)> {
        let aliases = self.extract_aliases();
        let mut edges = vec![];
        for alias in &aliases {
            let all_aggregations = alias.extract_aggregations();
            for aggr in all_aggregations {
                edges.push((
                    CalculationNode::AggregationAlias(alias.clone()),
                    CalculationNode::Aggregation(aggr.clone()),
                ));
                for inner_expr in aggr.extract_aggregation_expressions() {
                    // if the expression matches the inner attribute then just create an edge between the aggregation and attribute
                    // it means that the attribute without any manipulation is the expression
                    if matches!(inner_expr, Expr::AttrUntyped(_)) {
                        edges.push((
                            CalculationNode::Aggregation(aggr.clone()),
                            CalculationNode::Attribute(inner_expr.clone()),
                        ));
                    } else {
                        edges.push((
                            CalculationNode::Aggregation(aggr.clone()),
                            CalculationNode::Expression(inner_expr.clone()),
                        ));
                        for attr in inner_expr.extract_attributes() {
                            edges.push((
                                CalculationNode::Expression(inner_expr.clone()),
                                CalculationNode::Attribute(attr.clone()),
                            ));
                        }
                    }
                }
            }
        }
        edges
    }

    pub fn extract_aggregation_expressions(&self) -> Vec<Expr> {
        let all_aggregations = self.extract_aggregations();
        let agg_expressions: Vec<&AggrExpr> = all_aggregations
            .iter()
            .map(|v| v.as_aggr().unwrap())
            .collect_vec();
        let mut all_projections = HashSet::new();
        agg_expressions.into_iter().for_each(|agg| {
            all_projections.insert(*agg.agg_expr.clone());
            if let Some(where_expr) = agg.cond.clone() {
                all_projections.insert(*where_expr.clone());
            }
            if let Some(groupby_expr) = agg.groupby.clone() {
                all_projections.insert(*groupby_expr.clone());
            }
            if let Some(having_expr) = agg.having.clone() {
                all_projections.insert(*having_expr.expr.clone());
            }
        });
        Vec::from_iter(all_projections.into_iter())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter, strum::Display))]
#[allow(dead_code)]
pub enum ExprFunc {
    // functions
    // math f64 - 1 to 1 translation from f64 module
    Floor(BExpr),
    Ceil(BExpr),
    Round(BExpr),
    Trunc(BExpr),
    Fract(BExpr),
    Abs(BExpr),
    Signum(BExpr),
    DivEuclid(BExpr, BExpr),
    RemEuclid(BExpr, BExpr),
    Powf(BExpr, BExpr),
    Sqrt(BExpr),
    Exp(BExpr),
    Exp2(BExpr),
    Ln(BExpr),
    Log(BExpr, BExpr),
    Log2(BExpr),
    Log10(BExpr),
    Sin(BExpr),
    Cos(BExpr),
    Tan(BExpr),
    Asin(BExpr),
    Acos(BExpr),
    Atan(BExpr),
    Expm1(BExpr),
    Ln1p(BExpr),
    Sinh(BExpr),
    Cosh(BExpr),
    Asinh(BExpr),
    Acosh(BExpr),
    Atanh(BExpr),
    Clamp(BExpr, BExpr, BExpr),

    // text functions
    Len(BExpr),
    Substr(BExpr, BExpr, BExpr),
    Concat(BExpr, BExpr),
    Trim(BExpr),
    Lower(BExpr),
    Upper(BExpr),
    Replace(BExpr, BExpr, BExpr),
    StartsWith(BExpr, BExpr),
    EndsWith(BExpr, BExpr),
    Contains(BExpr, BExpr),

    // regex functions
    RegexMatch(BExpr, BExpr),
    RegexExtract(BExpr, BExpr),
    RegexReplace(BExpr, BExpr, BExpr),
    RegexSplit(BExpr, BExpr),
    RegexCount(BExpr, BExpr),

    // null handling
    Coalesce(BExpr, BExpr),

    // date functions
    DateAdd(BExpr, BExpr),
    DateSub(BExpr, BExpr),
    Hour(BExpr),
    Minute(BExpr),
    Second(BExpr),
    Microsecond(BExpr),
    DatePart(BExpr, BExpr),
    Extract(BExpr, BExpr),
    FormatDate(BExpr, BExpr),
    Now,
    CurrentDate,
    CurrentTime,
    Date(BExpr),
    DateDiff(BExpr, BExpr),
    Year(BExpr),
    Month(BExpr),
    Day(BExpr),
    Week(BExpr),
    Weekday(BExpr),
    DayOfYear(BExpr),
    Quarter(BExpr),
    IsStartOfMonth(BExpr),
    IsEndOfMonth(BExpr),
    IsWeekend(BExpr),

    // control flow
    If(BExpr, BExpr, BExpr),
}

impl Display for ExprFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExprFunc::Floor(a) => write!(f, "floor({})", a),
            ExprFunc::Ceil(a) => write!(f, "ceil({})", a),
            ExprFunc::Round(a) => write!(f, "round({})", a),
            ExprFunc::Trunc(a) => write!(f, "trunc({})", a),
            ExprFunc::Fract(a) => write!(f, "fract({})", a),
            ExprFunc::Abs(a) => write!(f, "abs({})", a),
            ExprFunc::Signum(a) => write!(f, "signum({})", a),
            ExprFunc::DivEuclid(a, _) => write!(f, "div_euclid({})", a),
            ExprFunc::RemEuclid(a, _) => write!(f, "rem_euclid({})", a),
            ExprFunc::Powf(a, _) => write!(f, "powf({})", a),
            ExprFunc::Sqrt(a) => write!(f, "sqrt({})", a),
            ExprFunc::Exp(a) => write!(f, "expr({})", a),
            ExprFunc::Exp2(a) => write!(f, "exp2({})", a),
            ExprFunc::Ln(a) => write!(f, "ln({})", a),
            ExprFunc::Log(a, _) => write!(f, "log({})", a),
            ExprFunc::Log2(a) => write!(f, "log2({})", a),
            ExprFunc::Log10(a) => write!(f, "log10({})", a),
            ExprFunc::Sin(a) => write!(f, "sin({})", a),
            ExprFunc::Cos(a) => write!(f, "cos({})", a),
            ExprFunc::Tan(a) => write!(f, "tan({})", a),
            ExprFunc::Asin(a) => write!(f, "asin({})", a),
            ExprFunc::Acos(a) => write!(f, "acos({})", a),
            ExprFunc::Atan(a) => write!(f, "atan({})", a),
            ExprFunc::Expm1(a) => write!(f, "expm1({})", a),
            ExprFunc::Ln1p(a) => write!(f, "ln1p({})", a),
            ExprFunc::Sinh(a) => write!(f, "sinh({})", a),
            ExprFunc::Cosh(a) => write!(f, "cosh({})", a),
            ExprFunc::Asinh(a) => write!(f, "asinh({})", a),
            ExprFunc::Acosh(a) => write!(f, "acosh({})", a),
            ExprFunc::Atanh(a) => write!(f, "atanh({})", a),
            ExprFunc::Clamp(a, b, c) => write!(f, "clamp({}, {}, {})", a, b, c),
            ExprFunc::Len(a) => write!(f, "len({})", a),
            ExprFunc::Substr(a, b, c) => write!(f, "substr({}, {}, {})", a, b, c),
            ExprFunc::Concat(a, b) => write!(f, "concat({}, {})", a, b),
            ExprFunc::Trim(a) => write!(f, "trim({})", a),
            ExprFunc::Lower(a) => write!(f, "lower({})", a),
            ExprFunc::Upper(a) => write!(f, "upper({})", a),
            ExprFunc::Replace(a, b, c) => write!(f, "replace({}, {}, {})", a, b, c),
            ExprFunc::StartsWith(a, b) => write!(f, "startswith({}, {})", a, b),
            ExprFunc::EndsWith(a, b) => write!(f, "endsiwth({}, {})", a, b),
            ExprFunc::Contains(a, b) => write!(f, "contains({}, {})", a, b),
            ExprFunc::RegexMatch(a, b) => write!(f, "regex_match({}, {})", a, b),
            ExprFunc::RegexExtract(a, b) => write!(f, "regex_extract({}, {})", a, b),
            ExprFunc::RegexSplit(a, b) => write!(f, "regex_split({}, {})", a, b),
            ExprFunc::RegexCount(a, b) => write!(f, "regex_count({}, {})", a, b),
            ExprFunc::RegexReplace(a, b, c) => write!(f, "regex_replace({}, {}, {})", a, b, c),
            ExprFunc::Coalesce(a, b) => write!(f, "coalesce({}, {})", a, b),
            ExprFunc::DateAdd(a, b) => write!(f, "date_add({}, {})", a, b),
            ExprFunc::DateSub(a, b) => write!(f, "date_sub({}, {})", a, b),
            ExprFunc::Hour(a) => write!(f, "hour({})", a),
            ExprFunc::Minute(a) => write!(f, "minute({})", a),
            ExprFunc::Second(a) => write!(f, "second({})", a),
            ExprFunc::Microsecond(a) => write!(f, "microsecond({})", a),
            ExprFunc::DatePart(a, _) => write!(f, "date_part({})", a),
            ExprFunc::Extract(a, _) => write!(f, "extract({})", a),
            ExprFunc::FormatDate(a, _) => write!(f, "format_date({})", a),
            ExprFunc::Now => write!(f, "now()"),
            ExprFunc::CurrentDate => write!(f, "current_date()"),
            ExprFunc::CurrentTime => write!(f, "current_time()"),
            ExprFunc::Date(a) => write!(f, "date({})", a),
            ExprFunc::DateDiff(a, _) => write!(f, "date_diff({})", a),
            ExprFunc::Year(a) => write!(f, "year({})", a),
            ExprFunc::Month(a) => write!(f, "month({})", a),
            ExprFunc::Day(a) => write!(f, "day({})", a),
            ExprFunc::Week(a) => write!(f, "week({})", a),
            ExprFunc::Weekday(a) => write!(f, "weekday({})", a),
            ExprFunc::DayOfYear(a) => write!(f, "day_of_year({})", a),
            ExprFunc::Quarter(a) => write!(f, "quarter({})", a),
            ExprFunc::IsStartOfMonth(a) => write!(f, "is_start_of_month({})", a),
            ExprFunc::IsEndOfMonth(a) => write!(f, "is_end_of_month({})", a),
            ExprFunc::IsWeekend(a) => write!(f, "is_weekend({})", a),
            ExprFunc::If(a, b, c) => write!(f, "if({}, {}, {})", a, b, c),
        }
    }
}

impl Into<Feature> for Expr {
    fn into(self) -> Feature {
        match self {
            Expr::Alias(alias, expr) => Feature {
                raw: "".into(),
                expr: *expr.clone(),
                alias: Some(alias.to_string()),
            },
            _ => Feature {
                raw: "".into(),
                expr: self,
                alias: None,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FullQuery {
    pub select_exprs: Vec<Expr>,
}

impl Display for FullQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT\n")?;
        for expr in &self.select_exprs {
            write!(f, "    {}", expr)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SelectExpr {
    pub expressions: Vec<Expr>,
}

impl Display for SelectExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for expr in &self.expressions {
            write!(f, " {} ", expr)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum HavingExprType {
    MIN,
    MAX,
}

impl Display for HavingExprType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HavingExprType::MIN => write!(f, " min "),
            HavingExprType::MAX => write!(f, " max "),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HavingExpr {
    pub typ: HavingExprType,
    pub expr: BExpr,
}

impl Display for HavingExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, " {} {} ", self.typ, self.expr)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AggrExpr {
    pub agg_func: AggregateFunction,
    pub agg_expr: BExpr,
    pub from: Option<SmallString>,
    pub when: NewInterval,
    pub groupby: Option<BExpr>,
    pub cond: Option<BExpr>,
    pub having: Option<HavingExpr>,
}

impl Display for AggrExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.agg_func)?;
        write!(f, "({})", self.agg_expr)?;
        write!(f, " when {} ", self.when)?;
        if let Some(groupby) = &self.groupby {
            write!(f, " group by {} ", *groupby)?;
        }
        if let Some(cond) = &self.cond {
            write!(f, " where {} ", *cond)?;
        }
        if let Some(having) = &self.having {
            write!(f, " having {} ", having)?;
        }
        Ok(())
    }
}

/*
This data structure is only used for planning the execution of the expressions.
It doesn't have the aggregation applied because many aggregations can reuse
the same projection of data.
 */
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AggrProjectionExpr {
    pub agg_expr: BExpr,
    pub when: NewInterval,
    pub groupby: Option<BExpr>,
    pub cond: Option<BExpr>,
    pub having: Option<HavingExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub enum AggrExprFunction {
    Count(BExpr),
    Sum(BExpr),
    Min(BExpr),
    Max(BExpr),
    Avg(BExpr),
    Median(BExpr),
    Var(BExpr),
    StDev(BExpr),
    Last(BExpr),
    Nth(BExpr, BExpr),
    First(BExpr),
    TimeOfLast(BExpr),
    TimeOfNext(BExpr),
    AvgTimeBetween(BExpr),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    Median,
    Var,
    StDev,
    Last,
    Nth(BExpr),
    First,
    TimeOfLast,
    TimeOfFirst,
    TimeOfNext,
    AvgDaysBetween,
    Values,
}
