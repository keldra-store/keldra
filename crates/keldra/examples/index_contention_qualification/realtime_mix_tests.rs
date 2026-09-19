use super::realtime_request;

#[test]
fn realtime_request_mix_is_exact_in_each_hundred_request_cycle() {
    for percent in [0_u8, 30, 40, 50, 60, 70, 80, 90, 100] {
        let selected = (0..100)
            .filter(|sequence| realtime_request(percent, *sequence))
            .count();
        assert_eq!(selected, usize::from(percent));
    }
}

#[test]
fn realtime_request_mix_repeats_deterministically() {
    for sequence in 0..100 {
        assert_eq!(
            realtime_request(30, sequence),
            realtime_request(30, sequence + 100)
        );
    }
}
