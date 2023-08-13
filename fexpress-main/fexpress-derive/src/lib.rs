use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(DateUpdate)]
pub fn date_update_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct being derived on
    let name = &ast.ident;

    // Generate the implementation of the trait for the given struct
    let gen = quote! {
        impl #name {
            fn push_back(mut self, dt: NaiveDateTime) -> Self {
                if self.last_date.is_none() || dt >= self.last_date.expect("Partial aggregates can be only updated with later dates") {
                    self.first_date = Some(self.first_date.map_or(dt, |fd| fd.min(dt)));
                    self.last_date = Some(dt);
                }
                self
            }

            fn pop_front(mut self, dt: NaiveDateTime) -> Self {
                if self.first_date.is_none() || dt == self.first_date.expect("Partial aggregates can be only updated with earlier dates") {
                    self.last_date = Some(self.last_date.map_or(dt, |ld| ld.max(dt)));
                    self.first_date = Some(dt);
                }
                self
            }
        }
    };

    // Generate and return the final output
    gen.into()
}
