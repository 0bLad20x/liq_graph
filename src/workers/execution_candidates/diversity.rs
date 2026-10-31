//! CP9C: route-independent ranking of already observed alternative pools.
//! No RPC, wallet, amount veto, protocol decoding, or baseline score changes.
//! Slots are unique per candidate role; the caller applies exact V10 capacity.
use std::collections::{HashSet,VecDeque};

#[derive(Clone,Copy,Debug,PartialEq,Eq,serde::Serialize)]
#[serde(rename_all="snake_case")]
pub(super) enum Strategy {Baseline,Contrarian,Mixed,Coverage}
impl Strategy {
    pub(super) fn label(self)->&'static str {
        match self {Self::Baseline=>"baseline",Self::Contrarian=>"contrarian",Self::Mixed=>"mixed",Self::Coverage=>"coverage"}
    }
}

#[derive(Clone)]
pub(super) struct Alternative {
    pub slot:usize,
    pub role:usize,
    pub marginal_score:i128,
    /// Comparable only within the same token-input direction/role.
    pub depth_input_raw:Option<u128>,
    pub venue:&'static str,
}
/// Visit price-ordered extrema, then the midpoint of each remaining interval.
/// Never infer usable depth from a good infinitesimal rate.
fn spread_indices(indices:&[usize])->Vec<usize>{
    if indices.is_empty(){return Vec::new();}
    let mut output=Vec::with_capacity(indices.len());
    output.push(indices[0]);
    if indices.len()==1{return output;}
    output.push(*indices.last().unwrap());
    let mut queue=VecDeque::from([(0_usize,indices.len()-1)]);
    while let Some((start,end))=queue.pop_front(){
        if end<=start+1{continue;}
        let middle=start+(end-start)/2;
        output.push(indices[middle]);
        queue.push_back((start,middle));
        queue.push_back((middle,end));
    }
    output
}
fn round_robin(lanes:Vec<Vec<usize>>,len:usize)->Vec<usize>{
    let mut result=Vec::with_capacity(len);
    let mut seen=HashSet::new();
    for row in 0..len {
        for lane in &lanes {
            if let Some(&index)=lane.get(row){
                if seen.insert(index){result.push(index);}
            }
        }
        if result.len()==len{break;}
    }
    result
}
/// Count new oriented pool pairings offered by adding a candidate. This is
/// a coverage metric only, not a profitability or executable-size prediction.
pub(super) fn novelty(candidate_pool:usize, seed_pools:&[usize],
    reference_packs:&[Vec<usize>])->usize {
    let mut existing=HashSet::<(usize,usize)>::new();
    for pack in reference_packs {
        for &a in pack {for &b in pack {
            if a!=b {existing.insert((a,b));}
        }}
    }
    seed_pools.iter().filter(|&&other|other!=candidate_pool)
        .map(|&other|usize::from(!existing.contains(&(candidate_pool,other)))
            +usize::from(!existing.contains(&(other,candidate_pool)))).sum()
}
pub(super) fn coverage_order(alternatives:&[Alternative],seed_pools:&[usize],
    reference_packs:&[Vec<usize>])->Vec<usize> {
    let mut result=(0..alternatives.len()).collect::<Vec<_>>();
    result.sort_unstable_by_key(|&i|{
        let a=&alternatives[i];
        (std::cmp::Reverse(novelty(a.slot/2,seed_pools,reference_packs)),
            a.marginal_score,a.slot,a.role)
    });
    result
}
pub(super) fn order(strategy:Strategy,alternatives:&[Alternative])->Vec<usize> {
    let mut baseline=(0..alternatives.len()).collect::<Vec<_>>();
    baseline.sort_unstable_by_key(|&i|(alternatives[i].marginal_score,alternatives[i].slot,alternatives[i].role));
    if strategy==Strategy::Baseline{return baseline;}
    let mut lanes=Vec::new();
    // Keep role directions separate: a depth measured in SOL atoms must not
    // be ranked against depth measured in USDC atoms.
    for role in [0,1] {
        let mut indices=(0..alternatives.len()).filter(|&i|alternatives[i].role==role)
            .collect::<Vec<_>>();
        indices.sort_unstable_by_key(|&i|(alternatives[i].marginal_score,alternatives[i].slot));
        lanes.push(spread_indices(&indices));
    }
    if strategy==Strategy::Coverage {return baseline;}
    if strategy==Strategy::Contrarian {
        return round_robin(lanes,alternatives.len());
    }
    // Mixed: top marginal candidates, top input capacity per direction,
    // distinct venue representatives, and score-profile extremes.
    let mut depth_lanes=Vec::new();
    let mut venue_lanes=Vec::new();
    for role in [0,1] {
        let mut indices=(0..alternatives.len()).filter(|&i|alternatives[i].role==role).collect::<Vec<_>>();
        indices.sort_unstable_by_key(|&i|{
            (std::cmp::Reverse(alternatives[i].depth_input_raw.unwrap_or(0)),
                alternatives[i].marginal_score,alternatives[i].slot)
        });
        depth_lanes.push(indices.clone());
        let mut seen=HashSet::new();
        let mut venues=Vec::new();
        let mut rest=Vec::new();
        for index in indices {
            if seen.insert(alternatives[index].venue){venues.push(index);}
            else{rest.push(index);}
        }
        venues.extend(rest);venue_lanes.push(venues);
    }
    // One candidate of each observable feature on every round before any
    // individual feature can use the entire transaction account budget.
    let best_0=baseline.iter().copied().filter(|&i|alternatives[i].role==0).collect();
    let best_1=baseline.iter().copied().filter(|&i|alternatives[i].role==1).collect();
    round_robin(vec![best_0,best_1,depth_lanes.remove(0),depth_lanes.remove(0),
        venue_lanes.remove(0),venue_lanes.remove(0),lanes.remove(0),lanes.remove(0)],alternatives.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn alternatives()->Vec<Alternative> {
        (0..10).map(|i|Alternative{slot:i,role:i%2,marginal_score:i as i128*100,
            depth_input_raw:Some(if i==5{1_000_000}else{10+i as u128}),venue:if i%3==0{"a"}else{"b"}}).collect()
    }
    #[test]
    fn rankings_cover_every_candidate_once_and_are_deterministic() {
        let values=alternatives();
        for mode in [Strategy::Baseline,Strategy::Contrarian,Strategy::Mixed,Strategy::Coverage] {
            let order=order(mode,&values);
            assert_eq!(order.len(),values.len());
            assert_eq!(order.iter().copied().collect::<HashSet<_>>().len(),values.len());
            assert_eq!(order,super::order(mode,&values));
        }
    }
    #[test]
    fn contrarian_starts_with_both_price_extremes_per_direction() {
        let values=alternatives();let ranks=order(Strategy::Contrarian,&values);
        let first_four=ranks[..4].iter().map(|&i|values[i].slot).collect::<HashSet<_>>();
        assert_eq!(first_four,HashSet::from([0,8,1,9]));
    }
    #[test]
    fn complementary_pack_novelty_favors_uncovered_pairs() {
        let previous=vec![vec![1,2,3]];
        assert_eq!(novelty(3,&[1,2],&previous),0);
        assert_eq!(novelty(4,&[1,2],&previous),4);
        let alternatives=vec![
            Alternative{slot:6,role:0,marginal_score:0,depth_input_raw:None,venue:"x"},
            Alternative{slot:8,role:0,marginal_score:100,depth_input_raw:None,venue:"x"},
        ];
        assert_eq!(coverage_order(&alternatives,&[1,2],&previous),vec![1,0]);
    }
    #[test]
    fn mixed_prioritizes_distinct_profiles_not_only_best_marginal() {
        let values=alternatives();
        let mixed=order(Strategy::Mixed,&values);
        assert!(mixed[..8].contains(&5)); // the much deeper candidate
        assert_ne!(mixed,order(Strategy::Baseline,&values));
    }
}
