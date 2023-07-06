use std::env;
use anyhow::anyhow;
use futures::{future, stream, TryStreamExt};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;

use futures::stream::StreamExt;
use tokio::sync::broadcast::Receiver;

fn create_stream_from_channel(
    rx: Receiver<String>,
) -> impl futures::Stream<Item = Result<String, anyhow::Error>> {
    stream::unfold(rx, |mut rx_int| async move {
        let next_state = match rx_int.recv().await {
            Ok(event) => {
                if event == "Error" {
                    return Some((Err(anyhow!("Received error event")), rx_int));
                }
                Some(event)
            }
            Err(err) => return Some((Err(anyhow!(err)), rx_int)),
        };
        next_state.map(|next_state| (Ok(next_state), rx_int))
    })
}

//tokio main
#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", env::var("RUST_LOG").unwrap_or("info".into()));
    env_logger::init();

    let (tx, rx) = broadcast::channel(10);
    let rx2 = tx.subscribe();
    let event_task = tokio::task::spawn(async move {
        async move {
            //fancy method of capturing tx
            for event_no in 0..10 {
                sleep(Duration::from_secs(1)).await;
                if event_no == 5 {
                    if let Err(err) = tx.send("Error".into()) {
                        log::warn!(
                            "Error sending event, probably all receivers closed: {}",
                            err
                        );
                        break;
                    }
                } else if let Err(err) = tx.send(format!("Event no {event_no}")) {
                    log::warn!(
                        "Error sending event, probably all receivers closed: {}",
                        err
                    );
                    break;
                }
            }
            //tx is dropped here and rx will return immediately None
        }
        .await;
        log::info!("Done sending events, rx is already dropped");
    });

    let stream = create_stream_from_channel(rx);
    let stream2 = create_stream_from_channel(rx2);

    //consume stream in a imperative way
    tokio::task::spawn(async move {
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            match item {
                Ok(item) => {
                    log::info!("Received: {}", item);
                }
                Err(err) => {
                    log::error!("Error: {}", err);
                    break;
                }
            }
        }
    });


    //map stream2 to a new stream stream2
    let stream2 = stream2.map(|item| item.map(|item| item.replace("Event", "Modified event")));

    //consume stream2 in a functional way
    tokio::task::spawn(async move {
        match stream2
            .try_for_each(|item| async move {
                log::info!("Received: {}", item);
                Ok(())
            })
            .await
        {
            Ok(_) => {
                log::info!("Stream2 ended");
            }
            Err(err) => {
                log::error!("Stream2 ended with error: {}", err);
            }
        }
    });

    while !event_task.is_finished() {
        sleep(Duration::from_secs(1)).await;
    }
}