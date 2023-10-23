extern crate proc_macro;

use darling::export::NestedMeta;
use darling::FromMeta;
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::{
    parse2, spanned::Spanned, Expr, ExprLit, ExprPath, FnArg, Ident, ItemTrait, Lit, LitStr,
    TraitItem, TraitItemFn, Type,
};

#[derive(Debug, Default, Eq, PartialEq, FromMeta)]
struct LapinHooksArgs {
    pub no_value: Option<()>,
    pub value_name: Option<syn::Ident>,

    pub no_trait: Option<()>,
    pub trait_name: Option<syn::Ident>,
}

#[derive(Debug, Default, Eq, PartialEq, FromMeta)]
struct AmqpRouteArgs {
    pub path: Option<syn::Ident>,
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

impl AmqpRouteArgs {
    pub fn parse(attr: TokenStream) -> Result<Self, TokenStream> {
        let attr_args = NestedMeta::parse_meta_list(attr).map_err(|e| quote_spanned! {
            e.span() =>
                compile_error!(format!("Meta from attribute could not be parsed: {}", e).as_str());
        })?;

        AmqpRouteArgs::from_list(&attr_args).map_err(|e| {
            quote_spanned! {
                e.span() =>
                compile_error!("Args could not be parsed from meta: {}", e);
            }
        })
    }
}

#[proc_macro_attribute]
pub fn amqp_route(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    item
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
    } = parse2(item.clone())
        .unwrap_or_else(|_| unimplemented!("Typestate can only be created using enums."));

    let mut method_names: Vec<Ident> = Vec::new();
    let mut data_types: Vec<Type> = Vec::new();
    let mut route_declarations: Vec<Ident> = Vec::new();

    #[allow(clippy::never_loop)]
    for method in items {
        let TraitItem::Fn(TraitItemFn { sig, attrs, .. }) = method else {
            return quote_spanned! {
                brace_token.span =>
                compile_error!("Items on trait must only be functions.", e);
            }
            .into();
        };

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

        const AMQP_ROUTE_ATTRIBUTE: &str = "amqp_route";
        let Some(route_declaration) = attrs.into_iter().find(|attr| {
            attr.meta
                .path()
                .get_ident()
                .is_some_and(|path_ident| *path_ident == AMQP_ROUTE_ATTRIBUTE)
        }) else {
            return quote_spanned! {
                method_name.span() =>
                compile_error!(r#"Every function must have an "amqp_route" attribute."#);
            }
            .into();
        };

        let route_declaration_span = route_declaration.meta.span();

        let route_declaration = match route_declaration.meta {
            syn::Meta::List(nv) => nv,
            _ => {
                return quote_spanned! {
                    route_declaration_span =>
                    compile_error!(r#""amqp_route" attribute should be a list attribute - #[amqp_route(path = "...")]."#);
                }
                .into();
            }
        };

        let route_declaration_meta = match AmqpRouteArgs::parse(route_declaration.tokens) {
            Ok(val) => val,
            Err(err) => return err.into(),
        };

        let Some(route_declaration) = route_declaration_meta.path else {
            return quote_spanned! {
                route_declaration_span =>
                compile_error!(r#""amqp_route" attribute should have a path variable - #[amqp_route(path = "...")]."#);
            }
            .into();
        };

        method_names.push(method_name);
        data_types.push(data_type);
        route_declarations.push(route_declaration);
    }

    let impl_trait_ident = Ident::new(&trait_ident.to_string(), Span::call_site());
    let producer_name_string = format!("{}LapinProducer", trait_ident);
    let producer_name = Ident::new(&producer_name_string, Span::call_site());
    let producer_name_str = LitStr::new(&producer_name_string, Span::call_site());

    quote! {
        #item

        #vis struct #producer_name<'c> {
            #(#method_names: ::easymq::lapin::LapinProducer<'static, 'c, #data_types, fn(#data_types) -> Vec<u8>>,)*
        }

        impl<'c> ::std::fmt::Debug for #producer_name<'c> {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::DebugStruct::finish(&mut ::std::fmt::Formatter::debug_struct(f, #producer_name_str))
            }
        }

        impl<'c> #producer_name<'c> {
            async fn new(channel: &'c ::lapin::Channel) -> Result<#producer_name<'c>, ::lapin::Error> {
                Ok(Self {
                    #(#method_names: ::easymq::lapin::LapinProducer::new(channel, #route_declarations).await?,)*
                })
            }
        }

        #[async_trait::async_trait]
        impl<'c> #impl_trait_ident for #producer_name<'c> {
            #(async fn #method_names(&mut self, #method_names: #data_types) {
                ::easymq::Producer::<#data_types>::publish(&self.#method_names, #method_names)
                    .await
                    .expect("TODO fix error handling")
            })*
        }
    }
    .into()
}
