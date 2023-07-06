use std::env;
use anyhow::anyhow;
use futures::{FutureExt, stream, TryStreamExt};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;

use futures::stream::StreamExt;
use tokio::sync::broadcast::Receiver;

const MSG_ERROR_EVENT_STR: &str = "Dummy error event";

fn create_stream_from_channel(
    rx: Receiver<String>,
) -> impl futures::Stream<Item = Result<String, anyhow::Error>> {
    stream::unfold(rx, |mut rx_int| async move {
        let next_state = match rx_int.recv().await {
            Ok(event) => {
                if event == MSG_ERROR_EVENT_STR {
                    return Some((Err(anyhow!("Received error event: {}", event)), rx_int));
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

    const NUMBER_OF_EVENTS: u64 = 10;
    const EMIT_ERROR_ON_EVENT_NO: u64 = 5;

    let (tx, rx) = broadcast::channel(10);
    let rx2 = tx.subscribe();
    let event_task = tokio::task::spawn(async move {
        async move {
            //fancy method of capturing tx
            for event_no in 0..NUMBER_OF_EVENTS {
                if event_no == EMIT_ERROR_ON_EVENT_NO {
                    if let Err(err) = tx.send(MSG_ERROR_EVENT_STR.to_string()) {
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
                sleep(Duration::from_secs(1)).await;
            }
            //tx is dropped here and rx will return immediately None
        }
        .await;
        log::info!("Event emitter task ended. rx dropped.");
    });

    let stream1 = create_stream_from_channel(rx);
    let stream2 = create_stream_from_channel(rx2);

    //consume stream in a imperative way
    //it's quite easy and straightforward
    let consumer_task_1 = tokio::task::spawn(async move {
        futures::pin_mut!(stream1);
        while let Some(item) = stream1.next().await {
            match item {
                Ok(item) => {
                    log::info!("[Stream1] Received: {}", item);
                }
                Err(err) => {
                    log::error!("[Stream1] Error: {}", err);
                    break;
                }
            }
        }
        log::info!("Consumer task 1 ended");
    });


    //map stream2 to a new stream stream2
    let stream2 = stream2.map(|item| item.map(|item| item.replace("Event", "Modified event")));

    let stream2 = stream2.then(|item| async move {
        //do some async work here
        //note that it will block the stream if you spent too much time here
        tokio::time::sleep(Duration::from_millis(100)).await;
        item
    });

    //consume stream2 in a functional way
    //try for each is ending on first error, you can use for_each if you want to consume all items
    let consumer_task_2 = tokio::task::spawn(async move {
        match stream2
            .try_for_each(|item| async move {
                log::info!("[Stream2] Received: {}", item);
                Ok(())
            })
            .await
        {
            Ok(_) => {
                log::info!("[Stream2] ended");
            }
            Err(err) => {
                log::error!("[Stream2] ended with error: {}", err);
            }
        }
        log::info!("Consumer task 2 ended");
    });

    //this is not needed, but it will show that task is finished
    //if the program is stuck then probably logic of finishing task is flawed
    while !event_task.is_finished() {
        log::debug!("Waiting for event task to finish");
        sleep(Duration::from_secs(1)).await;
    }
    while !consumer_task_1.is_finished() {
        log::debug!("Waiting for consumer task 1 to finish");
        sleep(Duration::from_secs(1)).await;
    }
    while !consumer_task_2.is_finished() {
        log::debug!("Waiting for consumer task 2 to finish");
        sleep(Duration::from_secs(1)).await;
    }
}
