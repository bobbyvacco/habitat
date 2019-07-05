//! Periodically check membership rumors to automatically "time out"
//! `Suspect` rumors to `Confirmed`, and `Confirmed` rumors to
//! `Departed`. Also purge any rumors that have expired.

use crate::server::{timing::Timing,
                    Server};
use chrono::offset::Utc;
use habitat_common::liveliness_checker;
use std::{thread,
          time::Duration};

const LOOP_DELAY_MS: u64 = 500;

pub fn spawn_thread(name: String, server: Server, timing: Timing) -> std::io::Result<()> {
    thread::Builder::new().name(name)
                          .spawn(move || -> ! { run_loop(&server, &timing) })
                          .map(|_| ())
}

fn run_loop(server: &Server, timing: &Timing) -> ! {
    loop {
        liveliness_checker::mark_thread_alive().and_divergent();

        server.member_list
              .members_expired_to_confirmed_mlw(timing.suspicion_timeout_duration());

        server.member_list
              .members_expired_to_departed_mlw(timing.departure_timeout_duration());

        let now = Utc::now();
        server.departure_store.purge_expired(now);
        server.election_store.purge_expired(now);
        server.update_store.purge_expired(now);
        server.service_store.purge_expired(now);
        server.service_config_store.purge_expired(now);
        server.service_file_store.purge_expired(now);
        server.member_list.purge_expired_mlw(now);

        thread::sleep(Duration::from_millis(LOOP_DELAY_MS));
    }
}
