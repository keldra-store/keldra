async fn large_git_query_directory() -> MemoryDirectory {
    let mut builder = GitSourceSegmentBuilder::new(
        SegmentBuildOptions::new(16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for index in 0..12 {
        let mutation = IndexMutation::Upsert(GitSourceDocument {
            document: DocumentRef {
                path: format!("/source/{index:02}"),
                version: 1,
            },
            records: vec![GitSourceRecord {
                repository_id: "repo".into(),
                commit_id: "abc".into(),
                tree_path: format!("src/{index:02}.rs"),
                object_id: "shared-object".into(),
                pack_path: format!("/pack/{index:02}/{}", "x".repeat(440_000)),
                pack_version: 1,
                offset: 0,
                length: 1,
            }],
        });
        assert!(matches!(
            builder.try_push(mutation).unwrap(),
            SegmentPush::Accepted
        ));
    }
    let mut sink = MemoryBlockSink::default();
    let run = builder.seal(&mut sink).await.unwrap().unwrap();
    directory(&sink, run)
}

#[tokio::test]
async fn git_tree_page_rejects_retained_records_over_one_decode_budget() {
    let directory = large_git_query_directory().await;
    let result = GitSourceEngine::list_tree(&[directory], "repo", "abc", "src/", None, 12).await;
    assert!(matches!(result, Err(IndexError::ResourceLimit { .. })));
}

#[tokio::test]
async fn git_object_page_rejects_retained_records_over_one_decode_budget() {
    let directory = large_git_query_directory().await;
    let result = GitSourceEngine::get_object(&[directory], "repo", "shared-object", 12).await;
    assert!(matches!(result, Err(IndexError::ResourceLimit { .. })));
}
