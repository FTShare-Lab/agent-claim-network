//! 主题整合的镜像交付依赖：承接版本确认前保留来源，允许明确的后续整合链。

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::claim::{Claim, ClaimId, ClaimStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConsolidationDelivery {
    pub before: Claim,
    pub retired: Claim,
    pub carriers: Vec<Claim>,
}

pub(crate) fn acknowledge(acknowledged: &mut Vec<Claim>, claim: Claim) {
    acknowledged.retain(|c| c.id != claim.id);
    acknowledged.push(claim);
}

pub(crate) fn related_ids(groups: &[ConsolidationDelivery]) -> BTreeSet<ClaimId> {
    groups
        .iter()
        .flat_map(|g| {
            std::iter::once(g.retired.id.clone()).chain(g.carriers.iter().map(|c| c.id.clone()))
        })
        .collect()
}

pub(crate) fn ready(
    claim: &Claim,
    groups: &[ConsolidationDelivery],
    acknowledged: &[Claim],
    current: &[Claim],
) -> bool {
    groups.iter().filter(|g| g.retired == *claim).all(|group| {
        !group.carriers.is_empty()
            && group
                .carriers
                .iter()
                .all(|c| carrier_ready(c, groups, acknowledged, current, &mut BTreeSet::new()))
    })
}

fn carrier_ready(
    claim: &Claim,
    groups: &[ConsolidationDelivery],
    acknowledged: &[Claim],
    current: &[Claim],
    visiting: &mut BTreeSet<ClaimId>,
) -> bool {
    if claim.status != ClaimStatus::Deprecated
        && current.contains(claim)
        && acknowledged.contains(claim)
    {
        return true;
    }
    if !visiting.insert(claim.id.clone()) {
        return false;
    }
    let result = groups.iter().any(|g| {
        g.before == *claim
            && current.contains(&g.retired)
            && !g.carriers.is_empty()
            && g.carriers
                .iter()
                .all(|c| carrier_ready(c, groups, acknowledged, current, visiting))
    });
    visiting.remove(&claim.id);
    result
}

/// 后续普通修改不能绕过尚未兑现的整合依赖；恢复为有效 Claim 则撤销该弃用请求。
pub(crate) fn supersede(groups: &mut Vec<ConsolidationDelivery>, claim: &Claim) {
    if claim.status != ClaimStatus::Deprecated {
        groups.retain(|g| g.retired.id != claim.id);
    } else {
        for group in groups.iter_mut().filter(|g| g.retired.id == claim.id) {
            group.retired = claim.clone();
        }
    }
}

/// 保留待交付来源依赖的完整链；已交付且不再被引用的记录可以清理。
pub(crate) fn prune(
    groups: &mut Vec<ConsolidationDelivery>,
    acknowledged: &mut Vec<Claim>,
    pending: &[Claim],
) {
    let mut keep: BTreeSet<usize> = groups
        .iter()
        .enumerate()
        .filter(|(_, g)| pending.contains(&g.retired))
        .map(|(i, _)| i)
        .collect();
    loop {
        let mut next = keep.clone();
        for index in &keep {
            for carrier in &groups[*index].carriers {
                next.extend(
                    groups
                        .iter()
                        .enumerate()
                        .filter(|(_, g)| g.before == *carrier)
                        .map(|(i, _)| i),
                );
            }
        }
        if next == keep {
            break;
        }
        keep = next;
    }
    let mut index = 0;
    groups.retain(|_| {
        let retain = keep.contains(&index);
        index += 1;
        retain
    });
    let related = related_ids(groups);
    acknowledged.retain(|c| related.contains(&c.id));
}
