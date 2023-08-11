macro_rules! define_single_expr_eval_fn {
    ($name:ident, $pattern:pat => $inner:ident, $func:expr, $func_name:expr) => {
        pub fn $name(
            event: Option<&Event>,
            context: Option<&EvalContext>,
            stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
            expr: &BExpr,
        ) -> Result<Value> {
            let val = eval_simple_expr(expr, event, context, stored_variables)?;

            match val {
                $pattern => $func($inner),
                _ => {
                    let val_type: ValueType = val.into();
                    let msg = format!(
                        "Invalid argument for {}. It expects a compatible type. But the provided value type is {}",
                        $func_name, val_type
                    );
                    Err(anyhow!(msg))
                }
            }
        }
    };
}

macro_rules! define_double_expr_eval_fn {
    ($name:ident, $pattern1:pat => $inner1:ident, $pattern2:pat => $inner2:ident, $func:expr, $func_name:expr) => {
        pub fn $name(
            event: Option<&Event>,
            context: Option<&EvalContext>,
            stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
            expr1: &BExpr,
            expr2: &BExpr,
        ) -> Result<Value> {
            let val1 = eval_simple_expr(expr1, event, context, stored_variables)?;
            let val2 = eval_simple_expr(expr2, event, context, stored_variables)?;

            match (&val1, &val2) {
                ($pattern1, $pattern2) => $func($inner1, $inner2),
                _ => {
                    let val1_type: ValueType = val1.into();
                    let val2_type: ValueType = val2.into();
                    let msg = format!(
                        "Invalid arguments for {}. It expects compatible types. But the provided value types are {}, {}",
                        $func_name, val1_type, val2_type
                    );
                    Err(anyhow!(msg))
                }
            }
        }
    };
}
