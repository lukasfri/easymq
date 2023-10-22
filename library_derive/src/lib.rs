extern crate proc_macro;
use darling::export::NestedMeta;
use darling::FromMeta;
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::{
    parse2, spanned::Spanned, FnArg, Ident, ItemTrait, ReturnType, TraitItem, TraitItemFn, Type,
};

#[derive(Debug, Default, Eq, PartialEq, FromMeta)]
struct LapinHooksArgs {
    pub no_value: Option<()>,
    pub value_name: Option<syn::Ident>,

    pub no_trait: Option<()>,
    pub trait_name: Option<syn::Ident>,
}

impl LapinHooksArgs {
    pub fn parse(attr: TokenStream) -> Result<Self, TokenStream> {
        let attr_args = NestedMeta::parse_meta_list(attr).map_err(|e| quote_spanned! {
            e.span() =>
                compile_error!(format!("Meta from attribute could not be parsed: {}", e).as_str());
        })?;

        LapinHooksArgs::from_list(&attr_args).map_err(|e| {
            quote_spanned! {
                e.span() =>
                compile_error!("Args could not be parsed from meta: {}", e);
            }
        })
    }
}

#[proc_macro_attribute]
pub fn hooks_lapin_producer(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let _args = match LapinHooksArgs::parse(TokenStream::from(attr)) {
        Ok(val) => val,
        Err(err) => return err.into(),
    };
    let item = TokenStream::from(item);

    let ItemTrait {
        ident: trait_ident,
        vis,
        items,
        brace_token,
        ..
    } = parse2(item)
        .unwrap_or_else(|_| unimplemented!("Typestate can only be created using enums."));

    let mut method_names: Vec<Ident> = Vec::new();
    let mut data_types: Vec<Type> = Vec::new();

    #[allow(clippy::never_loop)]
    for method in items {
        let TraitItem::Fn(TraitItemFn { sig, .. }) = method else {
            return quote_spanned! {
                brace_token.span =>
                compile_error!("Items on trait must only be functions.", e);
            }
            .into();
        };

        let ReturnType::Type(_, ty) = sig.output else {
            return quote_spanned! {
                sig.fn_token.span =>
                compile_error!("Function must be async.");
            }
            .into();
        };

        return quote_spanned! {
            ty.span() =>
            compile_error!("Function must return Result. Type: {}", ty);
        }
        .into();

        let mut inputs = sig.inputs.into_iter();

        let Some(FnArg::Receiver(self_arg)) = inputs.next() else {
            return quote_spanned! {
                sig.paren_token.span =>
                compile_error!("Functions must have self argument.");
            }
            .into();
        };

        if self_arg.mutability.is_none() {
            return quote_spanned! {
                self_arg.self_token.span =>
                compile_error!("Functions must have mutable self argument.");
            }
            .into();
        }

        let Some(FnArg::Typed(data_arg)) = inputs.next() else {
            return quote_spanned! {
                self_arg.self_token.span =>
                compile_error!("Functions must have data argument.");
            }
            .into();
        };

        let method_name = sig.ident;
        let data_type = data_arg.ty.as_ref().clone();

        method_names.push(method_name);
        data_types.push(data_type);
    }

    let name = Ident::new(&format!("{}LapinProducer", trait_ident), Span::call_site());

    quote! {
        #vis struct #name {
            #(#method_names: #data_types),*
        }

        #[async_trait::async_trait]
        impl #trait_ident for #name {
        #(
            async fn #method_names(&mut self, #method_names: #data_types) {
                todo!()
            }
        )*

        }
    }
    .into()
}
