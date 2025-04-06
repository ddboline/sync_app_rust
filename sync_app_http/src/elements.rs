use dioxus::prelude::{component, dioxus_elements, rsx, Element, IntoDynNode, Props, VirtualDom};
use stack_string::StackString;
use std::path::Path;
use sync_app_lib::{
    calendar_sync::CalendarSync,
    garmin_sync::GarminSync,
    models::{FileSyncCache, FileSyncConfig},
    movie_sync::MovieSync,
    security_sync::SecuritySync,
    weather_sync::WeatherSync,
};

#[cfg(debug_assertions)]
use dioxus::prelude::{GlobalSignal, Readable};

use crate::errors::ServiceError as Error;

/// # Errors
/// Returns error if formatting fails
pub fn index_body(
    conf_list: Vec<FileSyncConfig>,
    entries: Vec<FileSyncCache>,
) -> Result<String, Error> {
    let mut app =
        VirtualDom::new_with_props(IndexElement, IndexElementProps { conf_list, entries });
    app.rebuild_in_place();
    let mut renderer = dioxus_ssr::Renderer::default();
    let mut buffer = String::new();
    renderer.render_to(&mut buffer, &app)?;
    Ok(buffer)
}

#[component]
fn IndexElement(conf_list: Vec<FileSyncConfig>, entries: Vec<FileSyncCache>) -> Element {
    let conf_element = conf_list.iter().enumerate().filter_map(|(idx, v)| {
        v.name.as_ref().map(|name| {
            rsx! {
                input {
                    key: "conf-key-{idx}",
                    "type": "button",
                    name: "sync-{name}",
                    value: "{name}",
                    "onclick": "syncName( '{name}' )",
                    br {},
                }
            }
        })
    });
    let entries = entries.iter().enumerate().map(|(idx, v)| {
        let id = v.id;
        let src = &v.src_url;
        let dst = &v.dst_url;

        rsx! {
            div {
                key: "entries-key-{idx}",
                input {
                    "type": "button",
                    name: "Rm",
                    value: "Rm",
                    "onclick": "removeCacheEntry('{id}')"
                },
                input {
                    "type": "button",
                    name: "DelSrc",
                    value: "DelSrc",
                    "onclick": "deleteEntry('{src}',
                    '{id}')"
                },
                "{src} {dst}",
                input {
                    "type": "button",
                    name: "DelDst",
                    value: "DelDst",
                    "onclick": "deleteEntry('{dst}',
                    '{id}')"
                },
                input {
                    "type": "button",
                    name: "Proc",
                    value: "Proc",
                    "onclick": "procCacheEntry('{id}')",
                },
            }
        }
    });
    let garmin_button = if Path::new(GarminSync::EXE_PATH).exists() {
        Some(rsx! {
            button {
                "type": "submit",
                name: "sync_garmin",
                "onclick": "heartrateSync();",
                "Scale Sync"
            },
        })
    } else {
        None
    };
    let movie_button = if Path::new(MovieSync::EXE_PATH).exists() {
        Some(rsx! {
            button {
                "type": "submit",
                name: "sync_movie",
                "onclick": "movieSync();",
                "Movie Sync"
            }
        })
    } else {
        None
    };
    let calendar_button = if Path::new(CalendarSync::EXE_PATH).exists() {
        Some(rsx! {
            button {
                "type": "submit",
                name: "sync_calendar",
                "onclick": "calendarSync();",
                "Calendar Sync"
            },
        })
    } else {
        None
    };
    let podcatch_button = if Path::new("/usr/bin/podcatch-rust").exists() {
        Some(rsx! {
            button {
                "type": "submit",
                name: "sync_podcasts",
                "onclick": "podcastSync();",
                "Podcast Sync"
            },
        })
    } else {
        None
    };
    let security_button = if Path::new(SecuritySync::EXE_PATH).exists() {
        Some(rsx! {
            button {
                "type": "submit",
                name: "sync_security",
                "onclick": "securitySync();",
                "Security Sync"
            },
        })
    } else {
        None
    };
    let weather_button = if Path::new(WeatherSync::EXE_PATH).exists() {
        Some(rsx! {
            button {
                "type": "submit",
                name: "sync_weather",
                "onclick": "weatherSync();",
                "Weather Sync"
            }
        })
    } else {
        None
    };
    rsx! {
        head {
            style {
                dangerous_inner_html: include_str!("../../templates/style.css")
            }
        },
        body {
            script {src: "/sync/scripts.js"},
            h3 {
                button {
                    "type": "submit",
                    name: "sync_button",
                    "onclick": "syncAll();",
                    "Sync"
                },
                button {
                    "type": "submit",
                    name: "proc_button",
                    "onclick": "processAll();",
                    "Process"
                },
                {garmin_button},
                {movie_button},
                {calendar_button},
                {podcatch_button},
                {security_button},
                {weather_button},
                button {
                    name: "garminconnectoutput",
                    id: "garminconnectoutput",
                    dangerous_inner_html: "&nbsp;"
                },
            },
            nav {
                id: "navigation",
                "start": "0",
                {conf_element},
            },
            article {
                id: "main_article",
                {entries},
            },
        }
    }
}

/// # Errors
/// Returns error if formatting fails
pub fn text_body(text: StackString) -> Result<String, Error> {
    let mut app = VirtualDom::new_with_props(TextElement, TextElementProps { text });
    app.rebuild_in_place();
    let mut renderer = dioxus_ssr::Renderer::default();
    let mut buffer = String::new();
    renderer.render_to(&mut buffer, &app)?;
    Ok(buffer)
}

#[component]
fn TextElement(text: StackString) -> Element {
    rsx! {
        textarea {
            autofocus: "true",
            readonly: "readonly",
            rows: "50",
            cols: "100",
            "{text}",
        }
    }
}
