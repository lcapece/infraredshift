import pandas as pd

from analyzer.plan_evidence import attach_join_plan_evidence


def test_plan_node_maps_to_join_and_aggregates_actual_detail_steps() -> None:
    joins = pd.DataFrame(
        [
            {
                "condition": "a.customer_id = p.customer_id",
                "left_physical_sources": "dev.raw.authorization_fact.customer_id",
                "right_physical_sources": "dev.raw.payment_instrument.customer_id",
                "physical_column_pairs": "dev.raw.authorization_fact.customer_id = dev.raw.payment_instrument.customer_id",
            }
        ]
    )
    explain = pd.DataFrame(
        [
            {
                "child_query_sequence": 1,
                "plan_node_id": 7,
                "plan_parent_id": 2,
                "plan_node": "XN Hash Join DS_BCAST_INNER",
                "plan_info": "Hash Cond: (a.customer_id = p.customer_id)",
            }
        ]
    )
    detail = pd.DataFrame(
        [
            {
                "child_query_sequence": 1,
                "plan_node_id": 7,
                "step_name": "hashjoin",
                "duration_s": 12,
                "input_rows": 1000,
                "output_rows": 40,
                "input_bytes": 80000,
                "output_bytes": 3200,
                "spilled_block_local_disk": 10,
                "spilled_block_remote_disk": 2,
                "remote_read_io": 7,
                "data_skewness": 30,
                "time_skewness": 25,
            }
        ]
    )

    result = attach_join_plan_evidence(joins, explain, detail).iloc[0]

    assert result["plan_node_id"] == 7
    assert result["plan_match_confidence"] == "high"
    assert result["actual_input_rows"] == 1000
    assert result["actual_remote_spill_blocks"] == 2
    assert result["actual_movement"] == "DS_BCAST_INNER"
