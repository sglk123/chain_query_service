use log::LevelFilter;
use log4rs::append::rolling_file::{
    policy::compound::trigger::size::SizeTrigger,
    policy::compound::CompoundPolicy,
    policy::compound::roll::fixed_window::FixedWindowRoller,
    RollingFileAppender
};
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;

pub fn init_log_with_default() {
    let roller = FixedWindowRoller::builder()
        .build("log/node.{}.log", 10)
        .unwrap();

    let trigger = SizeTrigger::new(1_000_000);

    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    let appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}{n}")))
        .build("server.log", Box::new(policy))
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("rolling_file", Box::new(appender)))
        .build(Root::builder().appender("rolling_file").build(LevelFilter::Info))
        .unwrap();

    log4rs::init_config(config).unwrap();
}