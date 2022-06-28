use proc_macro::TokenStream;
use quote::quote;
use syn::{self, Data};

#[proc_macro_derive(IntoValues)]
pub fn into_values(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_into_values(&ast)
}

fn impl_into_values(ast: &syn::DeriveInput) -> TokenStream {
    if let Data::Struct(data_struct) = &ast.data {
        // let field = data_struct.fields.iter().next().unwrap();
        let field = data_struct
            .fields
            .iter()
            .map(|f| f.ident.as_ref().expect("ToValues can only be derived for structs with named fields"))
            .collect::<Vec<_>>();
        let name = &ast.ident;
        let gen = quote! {
            impl IntoValues for #name {
                fn into_values(self) -> Vec<Value> {
                    vec![#(self.#field.into()),*]
                }
            }
        };
        gen.into()
    } else {
        panic!("ToValues can only be derived for structs");
    }
}