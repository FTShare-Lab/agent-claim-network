//! 自复核的引用、全文覆盖和版本约束回归；不把结构通过当作语义正确。
use super::*;

#[test]
fn dream_review_reports_exact_gaps_but_allows_sentence_separators() {
    let mut value = fixture().input;
    value["before"][0]["statement"] = json!("保留甲，保留乙；不得重试，revision >= 2");
    value["after"][0]["statement"] = value["before"][0]["statement"].clone();
    let validation = Validation::new(value).unwrap();
    let mut review = fixture_review(&validation);
    let preservation = review.preservation.remove(0);
    let support = review.support.remove(0);
    for text in ["保留甲", "保留乙", "不得重试", "revision >= 2"] {
        let mut p = preservation.clone();
        p.before.quote = text.into();
        review.preservation.push(p);
        let mut s = support.clone();
        s.after.quote = text.into();
        review.support.push(s);
    }
    validate_review(&validation, &review).unwrap();
    review
        .preservation
        .iter_mut()
        .find(|p| p.before.quote == "不得重试")
        .unwrap()
        .before
        .quote = "重试".into();
    let gaps = serde_json::to_value(coverage_gaps(&validation, &review)).unwrap();
    assert_eq!(gaps[0]["field"], "statement");
    assert_eq!(gaps[0]["missing_fragments"], json!(["不得"]));
    assert!(validate_review(&validation, &review).is_err());
    review
        .preservation
        .iter_mut()
        .find(|p| p.before.quote == "revision >= 2")
        .unwrap()
        .before
        .quote = "revision".into();
    let gaps = serde_json::to_string(&coverage_gaps(&validation, &review)).unwrap();
    assert!(gaps.contains(">="));
}

fn fixture() -> Validation {
    let before = json!({"id":"claim_11111111","name":"safe replay prerequisites","statement":"Query business status or use a contracted idempotency protocol.","scope":"unsafe replay","evidence_summary":"Known contract; timeout is not proof of failure.","status":"active","confidence":"medium"});
    Validation::new(json!({"group_id":"group","kind":"quality","before":[before.clone()],"after":[before],"evidence":{}})).unwrap()
}

#[test]
fn dream_review_rejects_omitted_alternative_even_with_valid_existing_quotes() {
    let validation = fixture();
    let mut review = fixture_review(&validation);
    review.preservation[0].before.quote = "Query business status".into();
    let error = validate_review(&validation, &review)
        .unwrap_err()
        .to_string();
    assert!(error.contains("cover all before") && error.contains("idempotency"));
}
#[test]
fn dream_review_rejects_uncovered_new_output_and_wrong_claim_quotes() {
    let validation = fixture();
    let review = fixture_review(&validation);
    validate_review(&validation, &review).unwrap();
    let mut bad = review.clone();
    bad.support[0].after.quote = "Query business status".into();
    assert!(validate_review(&validation, &bad)
        .unwrap_err()
        .to_string()
        .contains("cover all after"));
    let mut bad = review.clone();
    bad.preservation[0].before.claim_id = "claim_22222222".parse().unwrap();
    assert!(validate_review(&validation, &bad).is_err());
    let mut bad = review;
    bad.support[0].before.clear();
    assert!(validate_review(&validation, &bad).is_err());
}
#[test]
fn dream_review_rejects_stale_version_and_deprecated_destinations() {
    let validation = fixture();
    let review = fixture_review(&validation);
    let mut changed = validation.input.clone();
    changed["after"][0]["scope"] = json!("another component");
    assert!(validate_review(&Validation::new(changed).unwrap(), &review).is_err());
    let mut changed = validation.clone();
    changed.input["after"][0]["status"] = json!("deprecated");
    assert!(validate_review(&changed, &review)
        .unwrap_err()
        .to_string()
        .contains("deprecated Claim"));
}
#[test]
fn dream_review_cannot_remove_factual_rules_without_new_evidence() {
    let validation = fixture();
    let mut review = fixture_review(&validation);
    review.preservation[0].disposition = Disposition::Corrected;
    assert!(validate_review(&validation, &review)
        .unwrap_err()
        .to_string()
        .contains("new direct evidence"));
    let mut validation = validation;
    validation.input["kind"] = json!("evidence");
    review.preservation[0].disposition = Disposition::Episodic;
    review.preservation[0].after.clear();
    assert!(validate_review(&validation, &review).is_err());
}
#[test]
fn dream_review_correction_checks_claim_specific_receipt_and_original_high() {
    let mut input = fixture().input;
    input["after"][0]["evidence_summary"] = json!("New observed contract.");
    input["evidence"] = json!({"file":{"tool":"file_read","output":{"file_version":{},"content":"Actual contract evidence."}}});
    input["change_basis"] = json!([{"claim_id":"claim_11111111","evidence_ids":["file"]}]);
    let mut validation = Validation::new(input).unwrap();
    let mut review = fixture_review(&validation);
    review.preservation[0].disposition = Disposition::Corrected;
    review.preservation[0].evidence = vec![EvidenceQuote {
        evidence_id: "file".into(),
        quote: "Actual contract evidence.".into(),
    }];
    validate_review(&validation, &review).unwrap();
    review.preservation[0].evidence[0].quote = "Invented result".into();
    assert!(validate_review(&validation, &review).is_err());
    review.preservation[0].evidence[0].quote = "Actual contract evidence.".into();
    validation.input["before"][0]["confidence"] = json!("high");
    assert!(validate_review(&validation, &review)
        .unwrap_err()
        .to_string()
        .contains("medium/low"));
}

fn numbered_evidence() -> Value {
    json!({"tool":"file_read","input":{"path":"src/queue.py"},"output":{
        "file_version":{},"content":"8|if revision >= 2:\n9|    return stored\n10|123|literal body\n",
        "page":{"returned_start":8,"returned_end":10}
    }})
}

#[test]
fn dream_review_accepts_body_quotes_without_display_line_numbers() {
    let mut input = fixture().input;
    input["evidence"]["file"] = numbered_evidence();
    let validation = Validation::new(input).unwrap();
    let mut review = fixture_review(&validation);
    for quote in [
        "if revision >= 2:\n    return stored",
        "  if revision >= 2:\n    return stored\n  ",
        "8|if revision >= 2:\n9|    return stored",
        "    return stored\n123|literal body",
    ] {
        review.support[0].evidence = vec![EvidenceQuote {
            evidence_id: "file".into(),
            quote: quote.into(),
        }];
        validate_review(&validation, &review).unwrap();
    }
    for quote in [
        "if revision > 2:\n    return stored",
        "if revision >= 2:\nreturn stored",
        "if revision >= 2:\n123|literal body",
        "return stored\nliteral body",
        "src/queue.py",
        "  \n ",
    ] {
        review.support[0].evidence[0].quote = quote.into();
        assert!(validate_review(&validation, &review).is_err(), "{quote}");
    }
}

#[test]
fn dream_review_does_not_strip_unconfirmed_line_prefixes() {
    let mut receipt = numbered_evidence();
    receipt["input"]["show_linenos"] = json!(false);
    assert_eq!(evidence_body(&receipt), evidence_text(&receipt));
    receipt["input"]["show_linenos"] = json!(true);
    receipt["output"]["page"]["returned_end"] = json!(11);
    assert_eq!(evidence_body(&receipt), evidence_text(&receipt));
    receipt["output"]["page"] = Value::Null;
    assert_eq!(evidence_body(&receipt), evidence_text(&receipt));
}

#[test]
fn dream_review_reports_all_bad_evidence_locations_and_bounded_source_previews() {
    let mut input = fixture().input;
    input["evidence"]["file"] = numbered_evidence();
    input["evidence"]["unversioned"] = json!({"tool":"file_read","input":{"path":"long.txt"},"output":{"content":"x".repeat(2000)}});
    let validation = Validation::new(input).unwrap();
    let mut review = fixture_review(&validation);
    review.preservation[0].evidence = vec![EvidenceQuote {
        evidence_id: "file".into(),
        quote: "wrong source text".into(),
    }];
    review.support[1].evidence = vec![
        EvidenceQuote {
            evidence_id: "file".into(),
            quote: "another bad quote".into(),
        },
        EvidenceQuote {
            evidence_id: "outside_group".into(),
            quote: "return stored".into(),
        },
        EvidenceQuote {
            evidence_id: "unversioned".into(),
            quote: "x".into(),
        },
    ];
    let feedback = evidence_feedback(&validation.input, &review);
    assert_eq!(feedback["evidence_errors"].as_array().unwrap().len(), 4);
    assert_eq!(feedback["evidence_errors"][0]["section"], "preservation");
    assert_eq!(feedback["evidence_errors"][2]["section"], "support");
    assert_eq!(feedback["evidence_errors"][2]["item_index"], 1);
    assert_eq!(feedback["evidence_errors"][2]["evidence_index"], 1);
    assert_eq!(feedback["evidence_previews"].as_object().unwrap().len(), 2);
    assert_eq!(
        feedback["evidence_previews"]["file"]["path"],
        "src/queue.py"
    );
    assert_eq!(
        feedback["evidence_previews"]["file"]["body_preview"],
        "if revision >= 2:\n    return stored\n123|literal body\n"
    );
    assert_eq!(
        feedback["evidence_previews"]["unversioned"]["preview_truncated"],
        true
    );
    assert_eq!(
        feedback["evidence_previews"]["unversioned"]["body_preview"]
            .as_str()
            .unwrap()
            .len(),
        1600
    );
    assert!(validate_review(&validation, &review).is_err());
}

#[test]
fn dream_review_requires_name_coverage_on_both_sides() {
    let mut input = fixture().input;
    input["after"][0]["name"] = json!("replay contract and status checks");
    let validation = Validation::new(input).unwrap();
    let complete = fixture_review(&validation);
    validate_review(&validation, &complete).unwrap();
    let mut review = complete.clone();
    review.preservation.retain(|p| p.before.field != "name");
    assert!(validate_review(&validation, &review).is_err());
    assert_eq!(
        serde_json::to_value(coverage_gaps(&validation, &review)).unwrap()[0]["field"],
        "name"
    );
    let mut review = complete;
    review.support.retain(|s| s.after.field != "name");
    assert!(validate_review(&validation, &review).is_err());
    assert_eq!(
        serde_json::to_value(coverage_gaps(&validation, &review)).unwrap()[0]["side"],
        "after"
    );
}
