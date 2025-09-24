DROP SCHEMA IF EXISTS core CASCADE;

CREATE SCHEMA IF NOT EXISTS core;

-- View: core.initial_asmt_summary

DROP MATERIALIZED VIEW IF EXISTS core.initial_asmt_summary;

-- store transformation output as view
CREATE MATERIALIZED VIEW IF NOT EXISTS core.initial_asmt_summary
TABLESPACE pg_default
AS
 WITH neighbour_la AS ( -- select only records of neighbouring councils
         SELECT tab_a1.system_id,
            tab_a1.local_authority,
            tab_a1.initial_assessments,
            tab_a1.owed_prevention_or_relief_duty,
            tab_a1.prevention_duty_owed,
            tab_a1.relief_duty_owed,
            tab_a1.households_in_area_000s,
            tab_a1.quarter_ending,
            tab_a1.is_neighbouring_la,
            "left"(tab_a1.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(tab_a1.quarter_ending::text, 2)::integer AS qtr_month
           FROM staging.tab_a1
          WHERE tab_a1.is_neighbouring_la = 1
        ), quarterly_avg AS ( -- calculate aggregated metrics (average) for neighbouring council records
         SELECT round(avg(neighbour_la.initial_assessments), 0) AS initial_assessments,
            round(avg(neighbour_la.owed_prevention_or_relief_duty), 0) AS owed_prevention_or_relief_duty,
            round(avg(neighbour_la.prevention_duty_owed), 0) AS prevention_duty_owed,
            round(avg(neighbour_la.relief_duty_owed), 0) AS relief_duty_owed,
            round(avg(neighbour_la.households_in_area_000s), 3) AS households_in_area_000s,
            neighbour_la.quarter_ending
           FROM neighbour_la
          GROUP BY neighbour_la.quarter_ending
        ), neighbour_quarterly_avg AS ( -- generate a group system_id and local_authority label for neighbouring councils
         SELECT 'N001'::text AS system_id,
            'Neighbouring LA'::text AS local_authority,
            quarterly_avg.initial_assessments,
            quarterly_avg.owed_prevention_or_relief_duty,
            quarterly_avg.prevention_duty_owed,
            quarterly_avg.relief_duty_owed,
            quarterly_avg.households_in_area_000s,
            quarterly_avg.quarter_ending
           FROM quarterly_avg
        ), all_qtr AS ( -- append grouped neighbouring council records to hackney's for further analysis
         SELECT neighbour_quarterly_avg.system_id,
            neighbour_quarterly_avg.local_authority,
            neighbour_quarterly_avg.initial_assessments,
            neighbour_quarterly_avg.owed_prevention_or_relief_duty,
            neighbour_quarterly_avg.prevention_duty_owed,
            neighbour_quarterly_avg.relief_duty_owed,
            neighbour_quarterly_avg.households_in_area_000s,
            neighbour_quarterly_avg.quarter_ending
           FROM neighbour_quarterly_avg
        UNION  -- append to
         SELECT tab_a1.system_id,
            tab_a1.local_authority,
            tab_a1.initial_assessments,
            tab_a1.owed_prevention_or_relief_duty,
            tab_a1.prevention_duty_owed,
            tab_a1.relief_duty_owed,
            tab_a1.households_in_area_000s,
            tab_a1.quarter_ending
           FROM staging.tab_a1
          WHERE tab_a1.local_authority::text = 'Hackney'::text
        ), summary AS ( -- engineer percentage calculations and quarter year, month columns
         SELECT all_qtr.system_id,
            all_qtr.local_authority,
            all_qtr.initial_assessments,
            round(all_qtr.initial_assessments / (all_qtr.households_in_area_000s * 1000::numeric), 2) AS perc_initial_assessments,
            all_qtr.owed_prevention_or_relief_duty,
            round(all_qtr.owed_prevention_or_relief_duty / all_qtr.initial_assessments, 2) AS perc_owed_prevention_or_relief_duty,
            all_qtr.prevention_duty_owed,
            round(all_qtr.prevention_duty_owed / all_qtr.initial_assessments, 2) AS perc_prevention_duty_owed,
            all_qtr.relief_duty_owed,
            round(all_qtr.relief_duty_owed / all_qtr.initial_assessments, 2) AS perc_relief_duty_owed,
            all_qtr.households_in_area_000s,
            all_qtr.quarter_ending,
            "left"(all_qtr.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(all_qtr.quarter_ending::text, 2)::integer AS qtr_month
           FROM all_qtr
          ORDER BY all_qtr.quarter_ending, all_qtr.local_authority
        ), quarter_ind AS ( -- engineer year_qtr indicator column to enhance axis sorting in dashboard visuals
         SELECT summary.system_id,
            summary.local_authority,
            summary.initial_assessments,
            summary.perc_initial_assessments,
            summary.owed_prevention_or_relief_duty,
            summary.perc_owed_prevention_or_relief_duty,
            summary.prevention_duty_owed,
            summary.perc_prevention_duty_owed,
            summary.relief_duty_owed,
            summary.perc_relief_duty_owed,
            round(summary.households_in_area_000s * 1000::numeric, 0) AS households_in_area,
            summary.quarter_ending,
            summary.qtr_year,
            summary.qtr_month,
                CASE
                    WHEN summary.qtr_month = 3 THEN 'q1'::text
                    WHEN summary.qtr_month = 6 THEN 'q2'::text
                    WHEN summary.qtr_month = 9 THEN 'q3'::text
                    WHEN summary.qtr_month = 12 THEN 'q4'::text
                    ELSE 'q0'::text
                END AS year_qtr
           FROM summary
        ), unpivot_table AS ( -- unpivot all metric columns into a categorical column (metrics) and values column (households)
         SELECT quarter_ind.local_authority,
            quarter_ind.quarter_ending,
            quarter_ind.qtr_year,
            quarter_ind.year_qtr,
            quarter_ind.qtr_month,
            jsonb_each_text.key AS metrics,
            jsonb_each_text.value::numeric AS households
           FROM quarter_ind,
            LATERAL jsonb_each_text(to_jsonb(quarter_ind.*) - 'local_authority'::text - 'system_id'::text - 'quarter_ending'::text - 'qtr_year'::text - 'qtr_month'::text - 'year_qtr'::text) jsonb_each_text(key, value)
        ), pivot_la AS ( -- pivot local_authority values into columns with households column as values
         SELECT unpivot_table.quarter_ending,
            unpivot_table.qtr_year,
            unpivot_table.year_qtr,
            unpivot_table.qtr_month,
            unpivot_table.metrics,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Hackney'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS hackney_la,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Neighbouring LA'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS neighbouring_la
           FROM unpivot_table
          GROUP BY unpivot_table.quarter_ending, unpivot_table.qtr_year, unpivot_table.year_qtr, unpivot_table.qtr_month, unpivot_table.metrics
          ORDER BY unpivot_table.quarter_ending
        )
 SELECT quarter_ending,
    qtr_year,
    year_qtr,
    qtr_month,
    metrics,
    hackney_la,
    neighbouring_la,
    hackney_la - neighbouring_la AS difference
   FROM pivot_la
WITH DATA;

ALTER TABLE IF EXISTS core.initial_asmt_summary
    OWNER TO postgres;

-- View: core.prevention_duty_summary

DROP MATERIALIZED VIEW IF EXISTS core.prevention_duty_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.prevention_duty_summary
TABLESPACE pg_default
AS
 WITH neighbour_la AS (
         SELECT tab_a2p.system_id,
            tab_a2p.local_authority,
            tab_a2p.prevention_duty_owed,
            tab_a2p.family_or_friend_terminations,
            tab_a2p.ast_private_rented_terminations,
            tab_a2p.domestic_abuse_terminations,
            tab_a2p.non_violent_relationship_breakdown_terminations,
            tab_a2p.social_rented_tenancy_terminations,
            tab_a2p.supported_housing_terminations,
            tab_a2p.non_ast_private_rented_terminations,
            tab_a2p.other_violence_or_harassment_terminations,
            tab_a2p.institution_departures,
            tab_a2p.home_office_asylum_support_terminations,
            tab_a2p.new_home_for_illness_or_disability,
            tab_a2p.loss_of_placement_or_sponsorship,
            tab_a2p.for_other_or_unknown_reasons,
            tab_a2p.quarter_ending,
            tab_a2p.is_neighbouring_la,
            "left"(tab_a2p.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(tab_a2p.quarter_ending::text, 2)::integer AS qtr_month
           FROM staging.tab_a2p
          WHERE tab_a2p.is_neighbouring_la = 1
        ), quarterly_avg AS (
         SELECT round(avg(neighbour_la.prevention_duty_owed), 0) AS prevention_duty_owed,
            round(avg(neighbour_la.family_or_friend_terminations), 0) AS family_or_friend_terminations,
            round(avg(neighbour_la.ast_private_rented_terminations), 0) AS ast_private_rented_terminations,
            round(avg(neighbour_la.domestic_abuse_terminations), 0) AS domestic_abuse_terminations,
            round(avg(neighbour_la.non_violent_relationship_breakdown_terminations), 0) AS non_violent_relationship_breakdown_terminations,
            round(avg(neighbour_la.social_rented_tenancy_terminations), 0) AS social_rented_tenancy_terminations,
            round(avg(neighbour_la.supported_housing_terminations), 0) AS supported_housing_terminations,
            round(avg(neighbour_la.non_ast_private_rented_terminations), 0) AS non_ast_private_rented_terminations,
            round(avg(neighbour_la.other_violence_or_harassment_terminations), 0) AS other_violence_or_harassment_terminations,
            round(avg(neighbour_la.institution_departures), 0) AS institution_departures,
            round(avg(neighbour_la.home_office_asylum_support_terminations), 0) AS home_office_asylum_support_terminations,
            round(avg(neighbour_la.new_home_for_illness_or_disability), 0) AS new_home_for_illness_or_disability,
            round(avg(neighbour_la.loss_of_placement_or_sponsorship), 0) AS loss_of_placement_or_sponsorship,
            round(avg(neighbour_la.for_other_or_unknown_reasons), 0) AS for_other_or_unknown_reasons,
            neighbour_la.quarter_ending
           FROM neighbour_la
          GROUP BY neighbour_la.quarter_ending
        ), neighbour_quarterly_avg AS (
         SELECT 'N001'::text AS system_id,
            'Neighbouring LA'::text AS local_authority,
            quarterly_avg.prevention_duty_owed,
            quarterly_avg.family_or_friend_terminations,
            quarterly_avg.ast_private_rented_terminations,
            quarterly_avg.domestic_abuse_terminations,
            quarterly_avg.non_violent_relationship_breakdown_terminations,
            quarterly_avg.social_rented_tenancy_terminations,
            quarterly_avg.supported_housing_terminations,
            quarterly_avg.non_ast_private_rented_terminations,
            quarterly_avg.other_violence_or_harassment_terminations,
            quarterly_avg.institution_departures,
            quarterly_avg.home_office_asylum_support_terminations,
            quarterly_avg.new_home_for_illness_or_disability,
            quarterly_avg.loss_of_placement_or_sponsorship,
            quarterly_avg.for_other_or_unknown_reasons,
            quarterly_avg.quarter_ending
           FROM quarterly_avg
        ), all_qtr AS (
         SELECT neighbour_quarterly_avg.system_id,
            neighbour_quarterly_avg.local_authority,
            neighbour_quarterly_avg.prevention_duty_owed,
            neighbour_quarterly_avg.family_or_friend_terminations,
            neighbour_quarterly_avg.ast_private_rented_terminations,
            neighbour_quarterly_avg.domestic_abuse_terminations,
            neighbour_quarterly_avg.non_violent_relationship_breakdown_terminations,
            neighbour_quarterly_avg.social_rented_tenancy_terminations,
            neighbour_quarterly_avg.supported_housing_terminations,
            neighbour_quarterly_avg.non_ast_private_rented_terminations,
            neighbour_quarterly_avg.other_violence_or_harassment_terminations,
            neighbour_quarterly_avg.institution_departures,
            neighbour_quarterly_avg.home_office_asylum_support_terminations,
            neighbour_quarterly_avg.new_home_for_illness_or_disability,
            neighbour_quarterly_avg.loss_of_placement_or_sponsorship,
            neighbour_quarterly_avg.for_other_or_unknown_reasons,
            neighbour_quarterly_avg.quarter_ending
           FROM neighbour_quarterly_avg
        UNION
         SELECT tab_a2p.system_id,
            tab_a2p.local_authority,
            tab_a2p.prevention_duty_owed,
            tab_a2p.family_or_friend_terminations,
            tab_a2p.ast_private_rented_terminations,
            tab_a2p.domestic_abuse_terminations,
            tab_a2p.non_violent_relationship_breakdown_terminations,
            tab_a2p.social_rented_tenancy_terminations,
            tab_a2p.supported_housing_terminations,
            tab_a2p.non_ast_private_rented_terminations,
            tab_a2p.other_violence_or_harassment_terminations,
            tab_a2p.institution_departures,
            tab_a2p.home_office_asylum_support_terminations,
            tab_a2p.new_home_for_illness_or_disability,
            tab_a2p.loss_of_placement_or_sponsorship,
            tab_a2p.for_other_or_unknown_reasons,
            tab_a2p.quarter_ending
           FROM staging.tab_a2p
          WHERE tab_a2p.local_authority::text = 'Hackney'::text
        ), summary AS (
         SELECT all_qtr.system_id,
            all_qtr.local_authority,
            all_qtr.prevention_duty_owed,
            all_qtr.family_or_friend_terminations AS pdo_from_family_or_friend_terminations,
            round(all_qtr.family_or_friend_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_family_friend_terminations,
            all_qtr.ast_private_rented_terminations AS pdo_from_ast_private_rented_terminations,
            round(all_qtr.ast_private_rented_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_ast_private_rented_terminations,
            all_qtr.domestic_abuse_terminations AS pdo_from_domestic_abuse_terminations,
            round(all_qtr.domestic_abuse_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_domestic_abuse_terminations,
            all_qtr.non_violent_relationship_breakdown_terminations AS pdo_from_non_violent_relationship_breakdown_terminations,
            round(all_qtr.non_violent_relationship_breakdown_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_non_violent_relationship_breakdown_terminations,
            all_qtr.social_rented_tenancy_terminations AS pdo_from_social_rented_tenancy_terminations,
            round(all_qtr.social_rented_tenancy_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_social_rented_tenancy_terminations,
            all_qtr.supported_housing_terminations AS pdo_from_supported_housing_terminations,
            round(all_qtr.supported_housing_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_supported_housing_terminations,
            all_qtr.non_ast_private_rented_terminations AS pdo_from_non_ast_private_rented_terminations,
            round(all_qtr.non_ast_private_rented_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_non_ast_private_rented_terminations,
            all_qtr.other_violence_or_harassment_terminations AS pdo_from_other_violence_or_harassment_terminations,
            round(all_qtr.other_violence_or_harassment_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_other_violence_or_harassment_terminations,
            all_qtr.institution_departures AS pdo_from_institution_departures,
            round(all_qtr.institution_departures / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_institution_departures,
            all_qtr.home_office_asylum_support_terminations AS pdo_from_home_office_asylum_support_terminations,
            round(all_qtr.home_office_asylum_support_terminations / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_home_office_asylum_support_terminations,
            all_qtr.new_home_for_illness_or_disability AS pdo_from_new_home_for_illness_or_disability,
            round(all_qtr.new_home_for_illness_or_disability / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_new_home_for_illness_or_disability,
            all_qtr.loss_of_placement_or_sponsorship AS pdo_from_loss_of_placement_or_sponsorship,
            round(all_qtr.loss_of_placement_or_sponsorship / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_loss_of_placement_or_sponsorship,
            all_qtr.for_other_or_unknown_reasons AS pdo_from_for_other_or_unknown_reasons,
            round(all_qtr.for_other_or_unknown_reasons / all_qtr.prevention_duty_owed, 2) AS perc_pdo_from_for_other_or_unknown_reasons,
            all_qtr.quarter_ending,
            "left"(all_qtr.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(all_qtr.quarter_ending::text, 2)::integer AS qtr_month
           FROM all_qtr
          ORDER BY all_qtr.quarter_ending, all_qtr.local_authority
        ), quarter_ind AS (
         SELECT summary.system_id,
            summary.local_authority,
            summary.prevention_duty_owed,
            summary.pdo_from_family_or_friend_terminations,
            summary.perc_pdo_from_family_friend_terminations,
            summary.pdo_from_ast_private_rented_terminations,
            summary.perc_pdo_from_ast_private_rented_terminations,
            summary.pdo_from_domestic_abuse_terminations,
            summary.perc_pdo_from_domestic_abuse_terminations,
            summary.pdo_from_non_violent_relationship_breakdown_terminations,
            summary.perc_pdo_from_non_violent_relationship_breakdown_terminations,
            summary.pdo_from_social_rented_tenancy_terminations,
            summary.perc_pdo_from_social_rented_tenancy_terminations,
            summary.pdo_from_supported_housing_terminations,
            summary.perc_pdo_from_supported_housing_terminations,
            summary.pdo_from_non_ast_private_rented_terminations,
            summary.perc_pdo_from_non_ast_private_rented_terminations,
            summary.pdo_from_other_violence_or_harassment_terminations,
            summary.perc_pdo_from_other_violence_or_harassment_terminations,
            summary.pdo_from_institution_departures,
            summary.perc_pdo_from_institution_departures,
            summary.pdo_from_home_office_asylum_support_terminations,
            summary.perc_pdo_from_home_office_asylum_support_terminations,
            summary.pdo_from_new_home_for_illness_or_disability,
            summary.perc_pdo_from_new_home_for_illness_or_disability,
            summary.pdo_from_loss_of_placement_or_sponsorship,
            summary.perc_pdo_from_loss_of_placement_or_sponsorship,
            summary.pdo_from_for_other_or_unknown_reasons,
            summary.perc_pdo_from_for_other_or_unknown_reasons,
            summary.quarter_ending,
            summary.qtr_year,
            summary.qtr_month,
                CASE
                    WHEN summary.qtr_month = 3 THEN 'q1'::text
                    WHEN summary.qtr_month = 6 THEN 'q2'::text
                    WHEN summary.qtr_month = 9 THEN 'q3'::text
                    WHEN summary.qtr_month = 12 THEN 'q4'::text
                    ELSE 'q0'::text
                END AS year_qtr
           FROM summary
        ), unpivot_table AS (
         SELECT quarter_ind.local_authority,
            quarter_ind.quarter_ending,
            quarter_ind.qtr_year,
            quarter_ind.year_qtr,
            quarter_ind.qtr_month,
            jsonb_each_text.key AS metrics,
            jsonb_each_text.value::numeric AS households
           FROM quarter_ind,
            LATERAL jsonb_each_text(to_jsonb(quarter_ind.*) - 'local_authority'::text - 'system_id'::text - 'quarter_ending'::text - 'qtr_year'::text - 'qtr_month'::text - 'year_qtr'::text) jsonb_each_text(key, value)
        ), pivot_la AS (
         SELECT unpivot_table.quarter_ending,
            unpivot_table.qtr_year,
            unpivot_table.year_qtr,
            unpivot_table.qtr_month,
            unpivot_table.metrics,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Hackney'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS hackney_la,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Neighbouring LA'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS neighbouring_la
           FROM unpivot_table
          GROUP BY unpivot_table.quarter_ending, unpivot_table.qtr_year, unpivot_table.year_qtr, unpivot_table.qtr_month, unpivot_table.metrics
          ORDER BY unpivot_table.quarter_ending
        )
 SELECT quarter_ending,
    qtr_year,
    year_qtr,
    qtr_month,
    metrics,
    hackney_la,
    neighbouring_la,
    hackney_la - neighbouring_la AS difference
   FROM pivot_la
WITH DATA;

ALTER TABLE IF EXISTS core.prevention_duty_summary
    OWNER TO postgres;

-- View: core.relief_duty_summary

DROP MATERIALIZED VIEW IF EXISTS core.relief_duty_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.relief_duty_summary
TABLESPACE pg_default
AS
 WITH neighbour_la AS (
         SELECT tab_a2r.system_id,
            tab_a2r.local_authority,
            tab_a2r.relief_duty_owed,
            tab_a2r.family_or_friend_terminations,
            tab_a2r.ast_private_rented_terminations,
            tab_a2r.domestic_abuse_terminations,
            tab_a2r.non_violent_relationship_breakdown_terminations,
            tab_a2r.social_rented_tenancy_terminations,
            tab_a2r.supported_housing_terminations,
            tab_a2r.non_ast_private_rented_terminations,
            tab_a2r.other_violence_or_harassment_terminations,
            tab_a2r.institution_departures,
            tab_a2r.home_office_asylum_support_terminations,
            tab_a2r.new_home_for_illness_or_disability,
            tab_a2r.loss_of_placement_or_sponsorship,
            tab_a2r.for_other_or_unknown_reasons,
            tab_a2r.quarter_ending,
            tab_a2r.is_neighbouring_la,
            "left"(tab_a2r.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(tab_a2r.quarter_ending::text, 2)::integer AS qtr_month
           FROM staging.tab_a2r
          WHERE tab_a2r.is_neighbouring_la = 1
        ), quarterly_avg AS (
         SELECT round(avg(neighbour_la.relief_duty_owed), 0) AS relief_duty_owed,
            round(avg(neighbour_la.family_or_friend_terminations), 0) AS family_or_friend_terminations,
            round(avg(neighbour_la.ast_private_rented_terminations), 0) AS ast_private_rented_terminations,
            round(avg(neighbour_la.domestic_abuse_terminations), 0) AS domestic_abuse_terminations,
            round(avg(neighbour_la.non_violent_relationship_breakdown_terminations), 0) AS non_violent_relationship_breakdown_terminations,
            round(avg(neighbour_la.social_rented_tenancy_terminations), 0) AS social_rented_tenancy_terminations,
            round(avg(neighbour_la.supported_housing_terminations), 0) AS supported_housing_terminations,
            round(avg(neighbour_la.non_ast_private_rented_terminations), 0) AS non_ast_private_rented_terminations,
            round(avg(neighbour_la.other_violence_or_harassment_terminations), 0) AS other_violence_or_harassment_terminations,
            round(avg(neighbour_la.institution_departures), 0) AS institution_departures,
            round(avg(neighbour_la.home_office_asylum_support_terminations), 0) AS home_office_asylum_support_terminations,
            round(avg(neighbour_la.new_home_for_illness_or_disability), 0) AS new_home_for_illness_or_disability,
            round(avg(neighbour_la.loss_of_placement_or_sponsorship), 0) AS loss_of_placement_or_sponsorship,
            round(avg(neighbour_la.for_other_or_unknown_reasons), 0) AS for_other_or_unknown_reasons,
            neighbour_la.quarter_ending
           FROM neighbour_la
          GROUP BY neighbour_la.quarter_ending
        ), neighbour_quarterly_avg AS (
         SELECT 'N001'::text AS system_id,
            'Neighbouring LA'::text AS local_authority,
            quarterly_avg.relief_duty_owed,
            quarterly_avg.family_or_friend_terminations,
            quarterly_avg.ast_private_rented_terminations,
            quarterly_avg.domestic_abuse_terminations,
            quarterly_avg.non_violent_relationship_breakdown_terminations,
            quarterly_avg.social_rented_tenancy_terminations,
            quarterly_avg.supported_housing_terminations,
            quarterly_avg.non_ast_private_rented_terminations,
            quarterly_avg.other_violence_or_harassment_terminations,
            quarterly_avg.institution_departures,
            quarterly_avg.home_office_asylum_support_terminations,
            quarterly_avg.new_home_for_illness_or_disability,
            quarterly_avg.loss_of_placement_or_sponsorship,
            quarterly_avg.for_other_or_unknown_reasons,
            quarterly_avg.quarter_ending
           FROM quarterly_avg
        ), all_qtr AS (
         SELECT neighbour_quarterly_avg.system_id,
            neighbour_quarterly_avg.local_authority,
            neighbour_quarterly_avg.relief_duty_owed,
            neighbour_quarterly_avg.family_or_friend_terminations,
            neighbour_quarterly_avg.ast_private_rented_terminations,
            neighbour_quarterly_avg.domestic_abuse_terminations,
            neighbour_quarterly_avg.non_violent_relationship_breakdown_terminations,
            neighbour_quarterly_avg.social_rented_tenancy_terminations,
            neighbour_quarterly_avg.supported_housing_terminations,
            neighbour_quarterly_avg.non_ast_private_rented_terminations,
            neighbour_quarterly_avg.other_violence_or_harassment_terminations,
            neighbour_quarterly_avg.institution_departures,
            neighbour_quarterly_avg.home_office_asylum_support_terminations,
            neighbour_quarterly_avg.new_home_for_illness_or_disability,
            neighbour_quarterly_avg.loss_of_placement_or_sponsorship,
            neighbour_quarterly_avg.for_other_or_unknown_reasons,
            neighbour_quarterly_avg.quarter_ending
           FROM neighbour_quarterly_avg
        UNION
         SELECT tab_a2r.system_id,
            tab_a2r.local_authority,
            tab_a2r.relief_duty_owed,
            tab_a2r.family_or_friend_terminations,
            tab_a2r.ast_private_rented_terminations,
            tab_a2r.domestic_abuse_terminations,
            tab_a2r.non_violent_relationship_breakdown_terminations,
            tab_a2r.social_rented_tenancy_terminations,
            tab_a2r.supported_housing_terminations,
            tab_a2r.non_ast_private_rented_terminations,
            tab_a2r.other_violence_or_harassment_terminations,
            tab_a2r.institution_departures,
            tab_a2r.home_office_asylum_support_terminations,
            tab_a2r.new_home_for_illness_or_disability,
            tab_a2r.loss_of_placement_or_sponsorship,
            tab_a2r.for_other_or_unknown_reasons,
            tab_a2r.quarter_ending
           FROM staging.tab_a2r
          WHERE tab_a2r.local_authority::text = 'Hackney'::text
        ), summary AS (
         SELECT all_qtr.system_id,
            all_qtr.local_authority,
            all_qtr.relief_duty_owed,
            all_qtr.family_or_friend_terminations AS rdo_from_family_or_friend_terminations,
            round(all_qtr.family_or_friend_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_family_friend_terminations,
            all_qtr.ast_private_rented_terminations AS rdo_from_ast_private_rented_terminations,
            round(all_qtr.ast_private_rented_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_ast_private_rented_terminations,
            all_qtr.domestic_abuse_terminations AS rdo_from_domestic_abuse_terminations,
            round(all_qtr.domestic_abuse_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_domestic_abuse_terminations,
            all_qtr.non_violent_relationship_breakdown_terminations AS rdo_from_non_violent_relationship_breakdown_terminations,
            round(all_qtr.non_violent_relationship_breakdown_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_non_violent_relationship_breakdown_terminations,
            all_qtr.social_rented_tenancy_terminations AS rdo_from_social_rented_tenancy_terminations,
            round(all_qtr.social_rented_tenancy_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_social_rented_tenancy_terminations,
            all_qtr.supported_housing_terminations AS rdo_from_supported_housing_terminations,
            round(all_qtr.supported_housing_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_supported_housing_terminations,
            all_qtr.non_ast_private_rented_terminations AS rdo_from_non_ast_private_rented_terminations,
            round(all_qtr.non_ast_private_rented_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_non_ast_private_rented_terminations,
            all_qtr.other_violence_or_harassment_terminations AS rdo_from_other_violence_or_harassment_terminations,
            round(all_qtr.other_violence_or_harassment_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_other_violence_or_harassment_terminations,
            all_qtr.institution_departures AS rdo_from_institution_departures,
            round(all_qtr.institution_departures / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_institution_departures,
            all_qtr.home_office_asylum_support_terminations AS rdo_from_home_office_asylum_support_terminations,
            round(all_qtr.home_office_asylum_support_terminations / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_home_office_asylum_support_terminations,
            all_qtr.new_home_for_illness_or_disability AS rdo_from_new_home_for_illness_or_disability,
            round(all_qtr.new_home_for_illness_or_disability / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_new_home_for_illness_or_disability,
            all_qtr.loss_of_placement_or_sponsorship AS rdo_from_loss_of_placement_or_sponsorship,
            round(all_qtr.loss_of_placement_or_sponsorship / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_loss_of_placement_or_sponsorship,
            all_qtr.for_other_or_unknown_reasons AS rdo_from_for_other_or_unknown_reasons,
            round(all_qtr.for_other_or_unknown_reasons / all_qtr.relief_duty_owed, 2) AS perc_rdo_from_for_other_or_unknown_reasons,
            all_qtr.quarter_ending,
            "left"(all_qtr.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(all_qtr.quarter_ending::text, 2)::integer AS qtr_month
           FROM all_qtr
          ORDER BY all_qtr.quarter_ending, all_qtr.local_authority
        ), quarter_ind AS (
         SELECT summary.system_id,
            summary.local_authority,
            summary.relief_duty_owed,
            summary.rdo_from_family_or_friend_terminations,
            summary.perc_rdo_from_family_friend_terminations,
            summary.rdo_from_ast_private_rented_terminations,
            summary.perc_rdo_from_ast_private_rented_terminations,
            summary.rdo_from_domestic_abuse_terminations,
            summary.perc_rdo_from_domestic_abuse_terminations,
            summary.rdo_from_non_violent_relationship_breakdown_terminations,
            summary.perc_rdo_from_non_violent_relationship_breakdown_terminations,
            summary.rdo_from_social_rented_tenancy_terminations,
            summary.perc_rdo_from_social_rented_tenancy_terminations,
            summary.rdo_from_supported_housing_terminations,
            summary.perc_rdo_from_supported_housing_terminations,
            summary.rdo_from_non_ast_private_rented_terminations,
            summary.perc_rdo_from_non_ast_private_rented_terminations,
            summary.rdo_from_other_violence_or_harassment_terminations,
            summary.perc_rdo_from_other_violence_or_harassment_terminations,
            summary.rdo_from_institution_departures,
            summary.perc_rdo_from_institution_departures,
            summary.rdo_from_home_office_asylum_support_terminations,
            summary.perc_rdo_from_home_office_asylum_support_terminations,
            summary.rdo_from_new_home_for_illness_or_disability,
            summary.perc_rdo_from_new_home_for_illness_or_disability,
            summary.rdo_from_loss_of_placement_or_sponsorship,
            summary.perc_rdo_from_loss_of_placement_or_sponsorship,
            summary.rdo_from_for_other_or_unknown_reasons,
            summary.perc_rdo_from_for_other_or_unknown_reasons,
            summary.quarter_ending,
            summary.qtr_year,
            summary.qtr_month,
                CASE
                    WHEN summary.qtr_month = 3 THEN 'q1'::text
                    WHEN summary.qtr_month = 6 THEN 'q2'::text
                    WHEN summary.qtr_month = 9 THEN 'q3'::text
                    WHEN summary.qtr_month = 12 THEN 'q4'::text
                    ELSE 'q0'::text
                END AS year_qtr
           FROM summary
        ), unpivot_table AS (
         SELECT quarter_ind.local_authority,
            quarter_ind.quarter_ending,
            quarter_ind.qtr_year,
            quarter_ind.year_qtr,
            quarter_ind.qtr_month,
            jsonb_each_text.key AS metrics,
            jsonb_each_text.value::numeric AS households
           FROM quarter_ind,
            LATERAL jsonb_each_text(to_jsonb(quarter_ind.*) - 'local_authority'::text - 'system_id'::text - 'quarter_ending'::text - 'qtr_year'::text - 'qtr_month'::text - 'year_qtr'::text) jsonb_each_text(key, value)
        ), pivot_la AS (
         SELECT unpivot_table.quarter_ending,
            unpivot_table.qtr_year,
            unpivot_table.year_qtr,
            unpivot_table.qtr_month,
            unpivot_table.metrics,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Hackney'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS hackney_la,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Neighbouring LA'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS neighbouring_la
           FROM unpivot_table
          GROUP BY unpivot_table.quarter_ending, unpivot_table.qtr_year, unpivot_table.year_qtr, unpivot_table.qtr_month, unpivot_table.metrics
          ORDER BY unpivot_table.quarter_ending
        )
 SELECT quarter_ending,
    qtr_year,
    year_qtr,
    qtr_month,
    metrics,
    hackney_la,
    neighbouring_la,
    hackney_la - neighbouring_la AS difference
   FROM pivot_la
WITH DATA;

ALTER TABLE IF EXISTS core.relief_duty_summary
    OWNER TO postgres;

-- View: core.prevention_duty_ending_summary

DROP MATERIALIZED VIEW IF EXISTS core.prevention_duty_ending_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.prevention_duty_ending_summary
TABLESPACE pg_default
AS
 WITH neighbour_la AS (
         SELECT tab_p1.system_id,
            tab_p1.local_authority,
            tab_p1.prevention_duty_ended,
            tab_p1.secured_accommodation,
            tab_p1.homelessness,
            tab_p1.contact_lost,
            tab_p1.no_further_action_after_56days,
            tab_p1.applicant_withdrew_or_deceased,
            tab_p1.no_longer_eligible,
            tab_p1.rejected_offered_accommodation,
            tab_p1.uncooperative,
            tab_p1.not_known,
            tab_p1.quarter_ending,
            tab_p1.is_neighbouring_la,
            "left"(tab_p1.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(tab_p1.quarter_ending::text, 2)::integer AS qtr_month
           FROM staging.tab_p1
          WHERE tab_p1.is_neighbouring_la = 1
        ), quarterly_avg AS (
         SELECT round(avg(neighbour_la.prevention_duty_ended), 0) AS prevention_duty_ended,
            round(avg(neighbour_la.secured_accommodation), 0) AS secured_accommodation,
            round(avg(neighbour_la.homelessness), 0) AS homelessness,
            round(avg(neighbour_la.contact_lost), 0) AS contact_lost,
            round(avg(neighbour_la.no_further_action_after_56days), 0) AS no_further_action_after_56days,
            round(avg(neighbour_la.applicant_withdrew_or_deceased), 0) AS applicant_withdrew_or_deceased,
            round(avg(neighbour_la.no_longer_eligible), 0) AS no_longer_eligible,
            round(avg(neighbour_la.rejected_offered_accommodation), 0) AS rejected_offered_accommodation,
            round(avg(neighbour_la.uncooperative), 0) AS uncooperative,
            round(avg(neighbour_la.not_known), 0) AS not_known,
            neighbour_la.quarter_ending
           FROM neighbour_la
          GROUP BY neighbour_la.quarter_ending
        ), neighbour_quarterly_avg AS (
         SELECT 'N001'::text AS system_id,
            'Neighbouring LA'::text AS local_authority,
            quarterly_avg.prevention_duty_ended,
            quarterly_avg.secured_accommodation,
            quarterly_avg.homelessness,
            quarterly_avg.contact_lost,
            quarterly_avg.no_further_action_after_56days,
            quarterly_avg.applicant_withdrew_or_deceased,
            quarterly_avg.no_longer_eligible,
            quarterly_avg.rejected_offered_accommodation,
            quarterly_avg.uncooperative,
            quarterly_avg.not_known,
            quarterly_avg.quarter_ending
           FROM quarterly_avg
        ), all_qtr AS (
         SELECT neighbour_quarterly_avg.system_id,
            neighbour_quarterly_avg.local_authority,
            neighbour_quarterly_avg.prevention_duty_ended,
            neighbour_quarterly_avg.secured_accommodation,
            neighbour_quarterly_avg.homelessness,
            neighbour_quarterly_avg.contact_lost,
            neighbour_quarterly_avg.no_further_action_after_56days,
            neighbour_quarterly_avg.applicant_withdrew_or_deceased,
            neighbour_quarterly_avg.no_longer_eligible,
            neighbour_quarterly_avg.rejected_offered_accommodation,
            neighbour_quarterly_avg.uncooperative,
            neighbour_quarterly_avg.not_known,
            neighbour_quarterly_avg.quarter_ending
           FROM neighbour_quarterly_avg
        UNION
         SELECT tab_p1.system_id,
            tab_p1.local_authority,
            tab_p1.prevention_duty_ended,
            tab_p1.secured_accommodation,
            tab_p1.homelessness,
            tab_p1.contact_lost,
            tab_p1.no_further_action_after_56days,
            tab_p1.applicant_withdrew_or_deceased,
            tab_p1.no_longer_eligible,
            tab_p1.rejected_offered_accommodation,
            tab_p1.uncooperative,
            tab_p1.not_known,
            tab_p1.quarter_ending
           FROM staging.tab_p1
          WHERE tab_p1.local_authority::text = 'Hackney'::text
        ), summary AS (
         SELECT all_qtr.system_id,
            all_qtr.local_authority,
            all_qtr.prevention_duty_ended,
            all_qtr.secured_accommodation AS pde_by_secured_accommodation,
            round(all_qtr.secured_accommodation / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_secured_accommodation,
            all_qtr.homelessness AS pde_by_homelessness,
            round(all_qtr.homelessness / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_homelessness,
            all_qtr.contact_lost AS pde_by_contact_lost,
            round(all_qtr.contact_lost / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_contact_lost,
            all_qtr.no_further_action_after_56days AS pde_by_no_further_action_after_56days,
            round(all_qtr.no_further_action_after_56days / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_no_further_action_after_56days,
            all_qtr.applicant_withdrew_or_deceased AS pde_by_applicant_withdrew_or_deceased,
            round(all_qtr.applicant_withdrew_or_deceased / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_applicant_withdrew_or_deceased,
            all_qtr.no_longer_eligible AS pde_by_no_longer_eligible,
            round(all_qtr.no_longer_eligible / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_no_longer_eligible,
            all_qtr.rejected_offered_accommodation AS pde_by_rejected_offered_accommodation,
            round(all_qtr.rejected_offered_accommodation / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_rejected_offered_accommodation,
            all_qtr.uncooperative AS pde_by_uncooperative,
            round(all_qtr.uncooperative / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_uncooperative,
            all_qtr.not_known AS pde_by_not_known,
            round(all_qtr.not_known / all_qtr.prevention_duty_ended, 2) AS perc_pde_by_not_known,
            all_qtr.quarter_ending,
            "left"(all_qtr.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(all_qtr.quarter_ending::text, 2)::integer AS qtr_month
           FROM all_qtr
          ORDER BY all_qtr.quarter_ending, all_qtr.local_authority
        ), quarter_ind AS (
         SELECT summary.system_id,
            summary.local_authority,
            summary.prevention_duty_ended,
            summary.pde_by_secured_accommodation,
            summary.perc_pde_by_secured_accommodation,
            summary.pde_by_homelessness,
            summary.perc_pde_by_homelessness,
            summary.pde_by_contact_lost,
            summary.perc_pde_by_contact_lost,
            summary.pde_by_no_further_action_after_56days,
            summary.perc_pde_by_no_further_action_after_56days,
            summary.pde_by_applicant_withdrew_or_deceased,
            summary.perc_pde_by_applicant_withdrew_or_deceased,
            summary.pde_by_no_longer_eligible,
            summary.perc_pde_by_no_longer_eligible,
            summary.pde_by_rejected_offered_accommodation,
            summary.perc_pde_by_rejected_offered_accommodation,
            summary.pde_by_uncooperative,
            summary.perc_pde_by_uncooperative,
            summary.pde_by_not_known,
            summary.perc_pde_by_not_known,
            summary.quarter_ending,
            summary.qtr_year,
            summary.qtr_month,
                CASE
                    WHEN summary.qtr_month = 3 THEN 'q1'::text
                    WHEN summary.qtr_month = 6 THEN 'q2'::text
                    WHEN summary.qtr_month = 9 THEN 'q3'::text
                    WHEN summary.qtr_month = 12 THEN 'q4'::text
                    ELSE 'q0'::text
                END AS year_qtr
           FROM summary
        ), unpivot_table AS (
         SELECT quarter_ind.local_authority,
            quarter_ind.quarter_ending,
            quarter_ind.qtr_year,
            quarter_ind.year_qtr,
            quarter_ind.qtr_month,
            jsonb_each_text.key AS metrics,
            jsonb_each_text.value::numeric AS households
           FROM quarter_ind,
            LATERAL jsonb_each_text(to_jsonb(quarter_ind.*) - 'local_authority'::text - 'system_id'::text - 'quarter_ending'::text - 'qtr_year'::text - 'qtr_month'::text - 'year_qtr'::text) jsonb_each_text(key, value)
        ), pivot_la AS (
         SELECT unpivot_table.quarter_ending,
            unpivot_table.qtr_year,
            unpivot_table.year_qtr,
            unpivot_table.qtr_month,
            unpivot_table.metrics,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Hackney'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS hackney_la,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Neighbouring LA'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS neighbouring_la
           FROM unpivot_table
          GROUP BY unpivot_table.quarter_ending, unpivot_table.qtr_year, unpivot_table.year_qtr, unpivot_table.qtr_month, unpivot_table.metrics
          ORDER BY unpivot_table.quarter_ending
        )
 SELECT quarter_ending,
    qtr_year,
    year_qtr,
    qtr_month,
    metrics,
    hackney_la,
    neighbouring_la,
    hackney_la - neighbouring_la AS difference
   FROM pivot_la
WITH DATA;

ALTER TABLE IF EXISTS core.prevention_duty_ending_summary
    OWNER TO postgres;

-- View: core.relief_duty_ending_summary

DROP MATERIALIZED VIEW IF EXISTS core.relief_duty_ending_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.relief_duty_ending_summary
TABLESPACE pg_default
AS
 WITH neighbour_la AS (
         SELECT tab_r1.system_id,
            tab_r1.local_authority,
            tab_r1.relief_duty_ended,
            tab_r1.secured_accommodation,
            tab_r1.after_56days_deadline,
            tab_r1.contact_lost,
            tab_r1.applicant_withdrew_or_deceased,
            tab_r1.rejected_final_accommodation_offered,
            tab_r1.intentionally_homeless_from_accommodation_provided,
            tab_r1.accepted_by_another_la,
            tab_r1.no_longer_eligible,
            tab_r1.uncooperative_and_served_notice,
            tab_r1.not_known,
            tab_r1.quarter_ending,
            tab_r1.is_neighbouring_la,
            "left"(tab_r1.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(tab_r1.quarter_ending::text, 2)::integer AS qtr_month
           FROM staging.tab_r1
          WHERE tab_r1.is_neighbouring_la = 1
        ), quarterly_avg AS (
         SELECT round(avg(neighbour_la.relief_duty_ended), 0) AS relief_duty_ended,
            round(avg(neighbour_la.secured_accommodation), 0) AS secured_accommodation,
            round(avg(neighbour_la.after_56days_deadline), 0) AS after_56days_deadline,
            round(avg(neighbour_la.contact_lost), 0) AS contact_lost,
            round(avg(neighbour_la.applicant_withdrew_or_deceased), 0) AS applicant_withdrew_or_deceased,
            round(avg(neighbour_la.rejected_final_accommodation_offered), 0) AS rejected_final_accommodation_offered,
            round(avg(neighbour_la.intentionally_homeless_from_accommodation_provided), 0) AS intentionally_homeless_from_accommodation_provided,
            round(avg(neighbour_la.accepted_by_another_la), 0) AS accepted_by_another_la,
            round(avg(neighbour_la.no_longer_eligible), 0) AS no_longer_eligible,
            round(avg(neighbour_la.uncooperative_and_served_notice), 0) AS uncooperative_and_served_notice,
            round(avg(neighbour_la.not_known), 0) AS not_known,
            neighbour_la.quarter_ending
           FROM neighbour_la
          GROUP BY neighbour_la.quarter_ending
        ), neighbour_quarterly_avg AS (
         SELECT 'N001'::text AS system_id,
            'Neighbouring LA'::text AS local_authority,
            quarterly_avg.relief_duty_ended,
            quarterly_avg.secured_accommodation,
            quarterly_avg.after_56days_deadline,
            quarterly_avg.contact_lost,
            quarterly_avg.applicant_withdrew_or_deceased,
            quarterly_avg.rejected_final_accommodation_offered,
            quarterly_avg.intentionally_homeless_from_accommodation_provided,
            quarterly_avg.accepted_by_another_la,
            quarterly_avg.no_longer_eligible,
            quarterly_avg.uncooperative_and_served_notice,
            quarterly_avg.not_known,
            quarterly_avg.quarter_ending
           FROM quarterly_avg
        ), all_qtr AS (
         SELECT neighbour_quarterly_avg.system_id,
            neighbour_quarterly_avg.local_authority,
            neighbour_quarterly_avg.relief_duty_ended,
            neighbour_quarterly_avg.secured_accommodation,
            neighbour_quarterly_avg.after_56days_deadline,
            neighbour_quarterly_avg.contact_lost,
            neighbour_quarterly_avg.applicant_withdrew_or_deceased,
            neighbour_quarterly_avg.rejected_final_accommodation_offered,
            neighbour_quarterly_avg.intentionally_homeless_from_accommodation_provided,
            neighbour_quarterly_avg.accepted_by_another_la,
            neighbour_quarterly_avg.no_longer_eligible,
            neighbour_quarterly_avg.uncooperative_and_served_notice,
            neighbour_quarterly_avg.not_known,
            neighbour_quarterly_avg.quarter_ending
           FROM neighbour_quarterly_avg
        UNION
         SELECT tab_r1.system_id,
            tab_r1.local_authority,
            tab_r1.relief_duty_ended,
            tab_r1.secured_accommodation,
            tab_r1.after_56days_deadline,
            tab_r1.contact_lost,
            tab_r1.applicant_withdrew_or_deceased,
            tab_r1.rejected_final_accommodation_offered,
            tab_r1.intentionally_homeless_from_accommodation_provided,
            tab_r1.accepted_by_another_la,
            tab_r1.no_longer_eligible,
            tab_r1.uncooperative_and_served_notice,
            tab_r1.not_known,
            tab_r1.quarter_ending
           FROM staging.tab_r1
          WHERE tab_r1.local_authority::text = 'Hackney'::text
        ), summary AS (
         SELECT all_qtr.system_id,
            all_qtr.local_authority,
            all_qtr.relief_duty_ended,
            all_qtr.secured_accommodation AS rde_by_secured_accommodation,
            round(all_qtr.secured_accommodation / all_qtr.relief_duty_ended, 2) AS perc_rde_by_secured_accommodation,
            all_qtr.after_56days_deadline AS rde_by_after_56days_deadline,
            round(all_qtr.after_56days_deadline / all_qtr.relief_duty_ended, 2) AS perc_rde_by_after_56days_deadline,
            all_qtr.contact_lost AS rde_by_contact_lost,
            round(all_qtr.contact_lost / all_qtr.relief_duty_ended, 2) AS perc_rde_by_contact_lost,
            all_qtr.applicant_withdrew_or_deceased AS rde_by_applicant_withdrew_or_deceased,
            round(all_qtr.applicant_withdrew_or_deceased / all_qtr.relief_duty_ended, 2) AS perc_rde_by_applicant_withdrew_or_deceased,
            all_qtr.rejected_final_accommodation_offered AS rde_by_rejected_final_accommodation_offered,
            round(all_qtr.rejected_final_accommodation_offered / all_qtr.relief_duty_ended, 2) AS perc_rde_by_rejected_final_accommodation_offered,
            all_qtr.intentionally_homeless_from_accommodation_provided AS rde_by_intentionally_homeless_from_accommodation_provided,
            round(all_qtr.intentionally_homeless_from_accommodation_provided / all_qtr.relief_duty_ended, 2) AS perc_rde_by_intentionally_homeless_from_accommodation_provided,
            all_qtr.accepted_by_another_la AS rde_by_accepted_by_another_la,
            round(all_qtr.accepted_by_another_la / all_qtr.relief_duty_ended, 2) AS perc_rde_by_accepted_by_another_la,
            all_qtr.no_longer_eligible AS rde_by_no_longer_eligible,
            round(all_qtr.no_longer_eligible / all_qtr.relief_duty_ended, 2) AS perc_rde_by_no_longer_eligible,
            all_qtr.uncooperative_and_served_notice AS rde_by_uncooperative_and_served_notice,
            round(all_qtr.uncooperative_and_served_notice / all_qtr.relief_duty_ended, 2) AS perc_rde_by_uncooperative_and_served_notice,
            all_qtr.not_known AS rde_by_not_known,
            round(all_qtr.not_known / all_qtr.relief_duty_ended, 2) AS perc_rde_by_not_known,
            all_qtr.quarter_ending,
            "left"(all_qtr.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(all_qtr.quarter_ending::text, 2)::integer AS qtr_month
           FROM all_qtr
          ORDER BY all_qtr.quarter_ending, all_qtr.local_authority
        ), quarter_ind AS (
         SELECT summary.system_id,
            summary.local_authority,
            summary.relief_duty_ended,
            summary.rde_by_secured_accommodation,
            summary.perc_rde_by_secured_accommodation,
            summary.rde_by_after_56days_deadline,
            summary.perc_rde_by_after_56days_deadline,
            summary.rde_by_contact_lost,
            summary.perc_rde_by_contact_lost,
            summary.rde_by_applicant_withdrew_or_deceased,
            summary.perc_rde_by_applicant_withdrew_or_deceased,
            summary.rde_by_rejected_final_accommodation_offered,
            summary.perc_rde_by_rejected_final_accommodation_offered,
            summary.rde_by_intentionally_homeless_from_accommodation_provided,
            summary.perc_rde_by_intentionally_homeless_from_accommodation_provided,
            summary.rde_by_accepted_by_another_la,
            summary.perc_rde_by_accepted_by_another_la,
            summary.rde_by_no_longer_eligible,
            summary.perc_rde_by_no_longer_eligible,
            summary.rde_by_uncooperative_and_served_notice,
            summary.perc_rde_by_uncooperative_and_served_notice,
            summary.rde_by_not_known,
            summary.perc_rde_by_not_known,
            summary.quarter_ending,
            summary.qtr_year,
            summary.qtr_month,
                CASE
                    WHEN summary.qtr_month = 3 THEN 'q1'::text
                    WHEN summary.qtr_month = 6 THEN 'q2'::text
                    WHEN summary.qtr_month = 9 THEN 'q3'::text
                    WHEN summary.qtr_month = 12 THEN 'q4'::text
                    ELSE 'q0'::text
                END AS year_qtr
           FROM summary
        ), unpivot_table AS (
         SELECT quarter_ind.local_authority,
            quarter_ind.quarter_ending,
            quarter_ind.qtr_year,
            quarter_ind.year_qtr,
            quarter_ind.qtr_month,
            jsonb_each_text.key AS metrics,
            jsonb_each_text.value::numeric AS households
           FROM quarter_ind,
            LATERAL jsonb_each_text(to_jsonb(quarter_ind.*) - 'local_authority'::text - 'system_id'::text - 'quarter_ending'::text - 'qtr_year'::text - 'qtr_month'::text - 'year_qtr'::text) jsonb_each_text(key, value)
        ), pivot_la AS (
         SELECT unpivot_table.quarter_ending,
            unpivot_table.qtr_year,
            unpivot_table.year_qtr,
            unpivot_table.qtr_month,
            unpivot_table.metrics,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Hackney'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS hackney_la,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Neighbouring LA'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS neighbouring_la
           FROM unpivot_table
          GROUP BY unpivot_table.quarter_ending, unpivot_table.qtr_year, unpivot_table.year_qtr, unpivot_table.qtr_month, unpivot_table.metrics
          ORDER BY unpivot_table.quarter_ending
        )
 SELECT quarter_ending,
    qtr_year,
    year_qtr,
    qtr_month,
    metrics,
    hackney_la,
    neighbouring_la,
    hackney_la - neighbouring_la AS difference
   FROM pivot_la
WITH DATA;

ALTER TABLE IF EXISTS core.relief_duty_ending_summary
    OWNER TO postgres;

-- View: core.temp_accommodation_summary

DROP MATERIALIZED VIEW IF EXISTS core.temp_accommodation_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.temp_accommodation_summary
TABLESPACE pg_default
AS
 WITH neighbour_la AS (
         SELECT tab_ta1.system_id,
            tab_ta1.local_authority,
            tab_ta1.households_in_ta,
            tab_ta1.ta_households_with_children,
            tab_ta1.children_headcount_in_ta,
            tab_ta1.bnb_ta_households,
            tab_ta1.bnb_ta_with_children,
            tab_ta1.bnb_ta_with_children_exceeding_6wks,
            tab_ta1.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal,
            tab_ta1.bnb_ta_with_16yo_17yo_main_applicant,
            tab_ta1.nightly_paid_ta_households,
            tab_ta1.nightly_paid_ta_with_children,
            tab_ta1.hostel_ta_households,
            tab_ta1.hostel_ta_with_children,
            tab_ta1.private_sector_ta,
            tab_ta1.private_sector_ta_with_children,
            tab_ta1.la_ha_owned_managed_ta_households,
            tab_ta1.la_ha_owned_managed_ta_with_children,
            tab_ta1.any_other_type_ta,
            tab_ta1.any_other_type_ta_with_children,
            tab_ta1.in_another_la_ta,
            tab_ta1.no_secured_accommodation_ta,
            tab_ta1.no_secured_accommodation_ta_with_children,
            tab_ta1.quarter_ending,
            tab_ta1.is_neighbouring_la,
            "left"(tab_ta1.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(tab_ta1.quarter_ending::text, 2)::integer AS qtr_month
           FROM staging.tab_ta1
          WHERE tab_ta1.is_neighbouring_la = 1
        ), quarterly_avg AS (
         SELECT round(avg(neighbour_la.households_in_ta), 0) AS households_in_ta,
            round(avg(neighbour_la.ta_households_with_children), 0) AS ta_households_with_children,
            round(avg(neighbour_la.children_headcount_in_ta), 0) AS children_headcount_in_ta,
            round(avg(neighbour_la.bnb_ta_households), 0) AS bnb_ta_households,
            round(avg(neighbour_la.bnb_ta_with_children), 0) AS bnb_ta_with_children,
            round(avg(neighbour_la.bnb_ta_with_children_exceeding_6wks), 0) AS bnb_ta_with_children_exceeding_6wks,
            round(avg(neighbour_la.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal), 0) AS bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal,
            round(avg(neighbour_la.bnb_ta_with_16yo_17yo_main_applicant), 0) AS bnb_ta_with_16yo_17yo_main_applicant,
            round(avg(neighbour_la.nightly_paid_ta_households), 0) AS nightly_paid_ta_households,
            round(avg(neighbour_la.nightly_paid_ta_with_children), 0) AS nightly_paid_ta_with_children,
            round(avg(neighbour_la.hostel_ta_households), 0) AS hostel_ta_households,
            round(avg(neighbour_la.hostel_ta_with_children), 0) AS hostel_ta_with_children,
            round(avg(neighbour_la.private_sector_ta), 0) AS private_sector_ta,
            round(avg(neighbour_la.private_sector_ta_with_children), 0) AS private_sector_ta_with_children,
            round(avg(neighbour_la.la_ha_owned_managed_ta_households), 0) AS la_ha_owned_managed_ta_households,
            round(avg(neighbour_la.la_ha_owned_managed_ta_with_children), 0) AS la_ha_owned_managed_ta_with_children,
            round(avg(neighbour_la.any_other_type_ta), 0) AS any_other_type_ta,
            round(avg(neighbour_la.any_other_type_ta_with_children), 0) AS any_other_type_ta_with_children,
            round(avg(neighbour_la.in_another_la_ta), 0) AS in_another_la_ta,
            round(avg(neighbour_la.no_secured_accommodation_ta), 0) AS no_secured_accommodation_ta,
            round(avg(neighbour_la.no_secured_accommodation_ta_with_children), 0) AS no_secured_accommodation_ta_with_children,
            neighbour_la.quarter_ending
           FROM neighbour_la
          GROUP BY neighbour_la.quarter_ending
        ), neighbour_quarterly_avg AS (
         SELECT 'N001'::text AS system_id,
            'Neighbouring LA'::text AS local_authority,
            quarterly_avg.households_in_ta,
            quarterly_avg.ta_households_with_children,
            quarterly_avg.children_headcount_in_ta,
            quarterly_avg.bnb_ta_households,
            quarterly_avg.bnb_ta_with_children,
            quarterly_avg.bnb_ta_with_children_exceeding_6wks,
            quarterly_avg.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal,
            quarterly_avg.bnb_ta_with_16yo_17yo_main_applicant,
            quarterly_avg.nightly_paid_ta_households,
            quarterly_avg.nightly_paid_ta_with_children,
            quarterly_avg.hostel_ta_households,
            quarterly_avg.hostel_ta_with_children,
            quarterly_avg.private_sector_ta,
            quarterly_avg.private_sector_ta_with_children,
            quarterly_avg.la_ha_owned_managed_ta_households,
            quarterly_avg.la_ha_owned_managed_ta_with_children,
            quarterly_avg.any_other_type_ta,
            quarterly_avg.any_other_type_ta_with_children,
            quarterly_avg.in_another_la_ta,
            quarterly_avg.no_secured_accommodation_ta,
            quarterly_avg.no_secured_accommodation_ta_with_children,
            quarterly_avg.quarter_ending
           FROM quarterly_avg
        ), all_qtr AS (
         SELECT neighbour_quarterly_avg.system_id,
            neighbour_quarterly_avg.local_authority,
            neighbour_quarterly_avg.households_in_ta,
            neighbour_quarterly_avg.ta_households_with_children,
            neighbour_quarterly_avg.children_headcount_in_ta,
            neighbour_quarterly_avg.bnb_ta_households,
            neighbour_quarterly_avg.bnb_ta_with_children,
            neighbour_quarterly_avg.bnb_ta_with_children_exceeding_6wks,
            neighbour_quarterly_avg.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal,
            neighbour_quarterly_avg.bnb_ta_with_16yo_17yo_main_applicant,
            neighbour_quarterly_avg.nightly_paid_ta_households,
            neighbour_quarterly_avg.nightly_paid_ta_with_children,
            neighbour_quarterly_avg.hostel_ta_households,
            neighbour_quarterly_avg.hostel_ta_with_children,
            neighbour_quarterly_avg.private_sector_ta,
            neighbour_quarterly_avg.private_sector_ta_with_children,
            neighbour_quarterly_avg.la_ha_owned_managed_ta_households,
            neighbour_quarterly_avg.la_ha_owned_managed_ta_with_children,
            neighbour_quarterly_avg.any_other_type_ta,
            neighbour_quarterly_avg.any_other_type_ta_with_children,
            neighbour_quarterly_avg.in_another_la_ta,
            neighbour_quarterly_avg.no_secured_accommodation_ta,
            neighbour_quarterly_avg.no_secured_accommodation_ta_with_children,
            neighbour_quarterly_avg.quarter_ending
           FROM neighbour_quarterly_avg
        UNION
         SELECT tab_ta1.system_id,
            tab_ta1.local_authority,
            tab_ta1.households_in_ta,
            tab_ta1.ta_households_with_children,
            tab_ta1.children_headcount_in_ta,
            tab_ta1.bnb_ta_households,
            tab_ta1.bnb_ta_with_children,
            tab_ta1.bnb_ta_with_children_exceeding_6wks,
            tab_ta1.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal,
            tab_ta1.bnb_ta_with_16yo_17yo_main_applicant,
            tab_ta1.nightly_paid_ta_households,
            tab_ta1.nightly_paid_ta_with_children,
            tab_ta1.hostel_ta_households,
            tab_ta1.hostel_ta_with_children,
            tab_ta1.private_sector_ta,
            tab_ta1.private_sector_ta_with_children,
            tab_ta1.la_ha_owned_managed_ta_households,
            tab_ta1.la_ha_owned_managed_ta_with_children,
            tab_ta1.any_other_type_ta,
            tab_ta1.any_other_type_ta_with_children,
            tab_ta1.in_another_la_ta,
            tab_ta1.no_secured_accommodation_ta,
            tab_ta1.no_secured_accommodation_ta_with_children,
            tab_ta1.quarter_ending
           FROM staging.tab_ta1
          WHERE tab_ta1.local_authority::text = 'Hackney'::text
        ), summary AS (
         SELECT all_qtr.system_id,
            all_qtr.local_authority,
            all_qtr.households_in_ta,
            all_qtr.ta_households_with_children,
            round(all_qtr.ta_households_with_children / all_qtr.households_in_ta, 2) AS perc_ta_households_with_children,
            all_qtr.children_headcount_in_ta,
            all_qtr.bnb_ta_households,
            round(all_qtr.bnb_ta_households / all_qtr.households_in_ta, 2) AS perc_bnb_ta_households,
            all_qtr.bnb_ta_with_children,
            round(all_qtr.bnb_ta_with_children / all_qtr.households_in_ta, 2) AS perc_bnb_ta_with_children,
            all_qtr.bnb_ta_with_children_exceeding_6wks,
            round(all_qtr.bnb_ta_with_children_exceeding_6wks / all_qtr.households_in_ta, 2) AS perc_bnb_ta_with_children_exceeding_6wks,
            all_qtr.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal,
            round(all_qtr.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal / all_qtr.households_in_ta, 2) AS perc_bnb_ta_with_children_exceeding_6wks_awaiting_review_or_app,
            all_qtr.bnb_ta_with_16yo_17yo_main_applicant,
            round(all_qtr.bnb_ta_with_16yo_17yo_main_applicant / all_qtr.households_in_ta, 2) AS perc_bnb_ta_with_16yo_17yo_main_applicant,
            all_qtr.nightly_paid_ta_households,
            round(all_qtr.nightly_paid_ta_households / all_qtr.households_in_ta, 2) AS perc_nightly_paid_ta_households,
            all_qtr.nightly_paid_ta_with_children,
            round(all_qtr.nightly_paid_ta_with_children / all_qtr.households_in_ta, 2) AS perc_nightly_paid_ta_with_children,
            all_qtr.hostel_ta_households,
            round(all_qtr.hostel_ta_households / all_qtr.households_in_ta, 2) AS perc_hostel_ta_households,
            all_qtr.hostel_ta_with_children,
            round(all_qtr.hostel_ta_with_children / all_qtr.households_in_ta, 2) AS perc_hostel_ta_with_children,
            all_qtr.private_sector_ta,
            round(all_qtr.private_sector_ta / all_qtr.households_in_ta, 2) AS perc_private_sector_ta,
            all_qtr.private_sector_ta_with_children,
            round(all_qtr.private_sector_ta_with_children / all_qtr.households_in_ta, 2) AS perc_private_sector_ta_with_children,
            all_qtr.la_ha_owned_managed_ta_households,
            round(all_qtr.la_ha_owned_managed_ta_households / all_qtr.households_in_ta, 2) AS perc_la_ha_owned_managed_ta_households,
            all_qtr.la_ha_owned_managed_ta_with_children,
            round(all_qtr.la_ha_owned_managed_ta_with_children / all_qtr.households_in_ta, 2) AS perc_la_ha_owned_managed_ta_with_children,
            all_qtr.any_other_type_ta,
            round(all_qtr.any_other_type_ta / all_qtr.households_in_ta, 2) AS perc_any_other_type_ta,
            all_qtr.any_other_type_ta_with_children,
            round(all_qtr.any_other_type_ta_with_children / all_qtr.households_in_ta, 2) AS perc_any_other_type_ta_with_children,
            all_qtr.in_another_la_ta,
            round(all_qtr.in_another_la_ta / all_qtr.households_in_ta, 2) AS perc_in_another_la_ta,
            all_qtr.no_secured_accommodation_ta,
            round(all_qtr.no_secured_accommodation_ta / all_qtr.households_in_ta, 2) AS perc_no_secured_accommodation_ta,
            all_qtr.no_secured_accommodation_ta_with_children,
            round(all_qtr.no_secured_accommodation_ta_with_children / all_qtr.households_in_ta, 2) AS perc_no_secured_accommodation_ta_with_children,
            all_qtr.quarter_ending,
            "left"(all_qtr.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(all_qtr.quarter_ending::text, 2)::integer AS qtr_month
           FROM all_qtr
          ORDER BY all_qtr.quarter_ending, all_qtr.local_authority
        ), quarter_ind AS (
         SELECT summary.system_id,
            summary.local_authority,
            summary.households_in_ta,
            summary.ta_households_with_children,
            summary.perc_ta_households_with_children,
            summary.children_headcount_in_ta,
            summary.bnb_ta_households,
            summary.perc_bnb_ta_households,
            summary.bnb_ta_with_children,
            summary.perc_bnb_ta_with_children,
            summary.bnb_ta_with_children_exceeding_6wks,
            summary.perc_bnb_ta_with_children_exceeding_6wks,
            summary.bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal,
            summary.perc_bnb_ta_with_children_exceeding_6wks_awaiting_review_or_app,
            summary.bnb_ta_with_16yo_17yo_main_applicant,
            summary.perc_bnb_ta_with_16yo_17yo_main_applicant,
            summary.nightly_paid_ta_households,
            summary.perc_nightly_paid_ta_households,
            summary.nightly_paid_ta_with_children,
            summary.perc_nightly_paid_ta_with_children,
            summary.hostel_ta_households,
            summary.perc_hostel_ta_households,
            summary.hostel_ta_with_children,
            summary.perc_hostel_ta_with_children,
            summary.private_sector_ta,
            summary.perc_private_sector_ta,
            summary.private_sector_ta_with_children,
            summary.perc_private_sector_ta_with_children,
            summary.la_ha_owned_managed_ta_households,
            summary.perc_la_ha_owned_managed_ta_households,
            summary.la_ha_owned_managed_ta_with_children,
            summary.perc_la_ha_owned_managed_ta_with_children,
            summary.any_other_type_ta,
            summary.perc_any_other_type_ta,
            summary.any_other_type_ta_with_children,
            summary.perc_any_other_type_ta_with_children,
            summary.in_another_la_ta,
            summary.perc_in_another_la_ta,
            summary.no_secured_accommodation_ta,
            summary.perc_no_secured_accommodation_ta,
            summary.no_secured_accommodation_ta_with_children,
            summary.perc_no_secured_accommodation_ta_with_children,
            summary.quarter_ending,
            summary.qtr_year,
            summary.qtr_month,
                CASE
                    WHEN summary.qtr_month = 3 THEN 'q1'::text
                    WHEN summary.qtr_month = 6 THEN 'q2'::text
                    WHEN summary.qtr_month = 9 THEN 'q3'::text
                    WHEN summary.qtr_month = 12 THEN 'q4'::text
                    ELSE 'q0'::text
                END AS year_qtr
           FROM summary
        ), unpivot_table AS (
         SELECT quarter_ind.local_authority,
            quarter_ind.quarter_ending,
            quarter_ind.qtr_year,
            quarter_ind.year_qtr,
            quarter_ind.qtr_month,
            jsonb_each_text.key AS metrics,
            jsonb_each_text.value::numeric AS households
           FROM quarter_ind,
            LATERAL jsonb_each_text(to_jsonb(quarter_ind.*) - 'local_authority'::text - 'system_id'::text - 'quarter_ending'::text - 'qtr_year'::text - 'qtr_month'::text - 'year_qtr'::text) jsonb_each_text(key, value)
        ), pivot_la AS (
         SELECT unpivot_table.quarter_ending,
            unpivot_table.qtr_year,
            unpivot_table.year_qtr,
            unpivot_table.qtr_month,
            unpivot_table.metrics,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Hackney'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS hackney_la,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Neighbouring LA'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS neighbouring_la
           FROM unpivot_table
          GROUP BY unpivot_table.quarter_ending, unpivot_table.qtr_year, unpivot_table.year_qtr, unpivot_table.qtr_month, unpivot_table.metrics
          ORDER BY unpivot_table.quarter_ending
        )
 SELECT quarter_ending,
    qtr_year,
    year_qtr,
    qtr_month,
    metrics,
    hackney_la,
    neighbouring_la,
    hackney_la - neighbouring_la AS difference
   FROM pivot_la
WITH DATA;

-- View: core.temp_accommodation_households_summary

DROP MATERIALIZED VIEW IF EXISTS core.temp_accommodation_households_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.temp_accommodation_households_summary
TABLESPACE pg_default
AS
 WITH neighbour_la AS (
         SELECT tab_ta2.system_id,
            tab_ta2.local_authority,
            tab_ta2.households_in_ta,
            tab_ta2.couple_with_children_ta,
            tab_ta2.single_father_with_children_ta,
            tab_ta2.single_mother_with_children_ta,
            tab_ta2.single_parent_of_other_unknown_gender_with_children_ta,
			tab_ta2.single_man_ta,
			tab_ta2.single_woman_ta,
			tab_ta2.single_other_gender_ta,
			tab_ta2.all_other_household_types_ta,
            tab_ta2.quarter_ending,
            tab_ta2.is_neighbouring_la,
            "left"(tab_ta2.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(tab_ta2.quarter_ending::text, 2)::integer AS qtr_month
           FROM staging.tab_ta2
          WHERE tab_ta2.is_neighbouring_la = 1
        ), quarterly_avg AS (
         SELECT round(avg(neighbour_la.households_in_ta), 0) AS households_in_ta,
            round(avg(neighbour_la.couple_with_children_ta), 0) AS couple_with_children_ta,
            round(avg(neighbour_la.single_father_with_children_ta), 0) AS single_father_with_children_ta,
            round(avg(neighbour_la.single_mother_with_children_ta), 0) AS single_mother_with_children_ta,
            round(avg(neighbour_la.single_parent_of_other_unknown_gender_with_children_ta), 0) AS single_parent_of_other_unknown_gender_with_children_ta,
			round(avg(neighbour_la.single_man_ta), 0) AS single_man_ta,
			round(avg(neighbour_la.single_woman_ta), 0) AS single_woman_ta,
			round(avg(neighbour_la.single_other_gender_ta), 0) AS single_other_gender_ta,
			round(avg(neighbour_la.all_other_household_types_ta), 0) AS all_other_household_types_ta,
            neighbour_la.quarter_ending
           FROM neighbour_la
          GROUP BY neighbour_la.quarter_ending
        ), neighbour_quarterly_avg AS (
         SELECT 'N001'::text AS system_id,
            'Neighbouring LA'::text AS local_authority,
            quarterly_avg.households_in_ta,
            quarterly_avg.couple_with_children_ta,
            quarterly_avg.single_father_with_children_ta,
            quarterly_avg.single_mother_with_children_ta,
            quarterly_avg.single_parent_of_other_unknown_gender_with_children_ta,
			quarterly_avg.single_man_ta,
			quarterly_avg.single_woman_ta,
			quarterly_avg.single_other_gender_ta,
			quarterly_avg.all_other_household_types_ta,
            quarterly_avg.quarter_ending
           FROM quarterly_avg
        ), all_qtr AS (
         SELECT neighbour_quarterly_avg.system_id,
            neighbour_quarterly_avg.local_authority,
            neighbour_quarterly_avg.households_in_ta,
            neighbour_quarterly_avg.couple_with_children_ta,
            neighbour_quarterly_avg.single_father_with_children_ta,
            neighbour_quarterly_avg.single_mother_with_children_ta,
            neighbour_quarterly_avg.single_parent_of_other_unknown_gender_with_children_ta,
			neighbour_quarterly_avg.single_man_ta,
			neighbour_quarterly_avg.single_woman_ta,
			neighbour_quarterly_avg.single_other_gender_ta,
			neighbour_quarterly_avg.all_other_household_types_ta,
            neighbour_quarterly_avg.quarter_ending
           FROM neighbour_quarterly_avg
        UNION
         SELECT tab_ta2.system_id,
            tab_ta2.local_authority,
            tab_ta2.households_in_ta,
            tab_ta2.couple_with_children_ta,
            tab_ta2.single_father_with_children_ta,
            tab_ta2.single_mother_with_children_ta,
            tab_ta2.single_parent_of_other_unknown_gender_with_children_ta,
			tab_ta2.single_man_ta,
			tab_ta2.single_woman_ta,
			tab_ta2.single_other_gender_ta,
			tab_ta2.all_other_household_types_ta,
            tab_ta2.quarter_ending
           FROM staging.tab_ta2
          WHERE tab_ta2.local_authority::text = 'Hackney'::text
        ), summary AS (
         SELECT all_qtr.system_id,
            all_qtr.local_authority,
            all_qtr.households_in_ta,
            all_qtr.couple_with_children_ta,
            round(all_qtr.couple_with_children_ta / all_qtr.households_in_ta, 2) AS perc_couple_with_children_ta,
            all_qtr.single_father_with_children_ta,
            round(all_qtr.single_father_with_children_ta / all_qtr.households_in_ta, 2) AS perc_single_father_with_children_ta,
            all_qtr.single_mother_with_children_ta,
            round(all_qtr.single_mother_with_children_ta / all_qtr.households_in_ta, 2) AS perc_single_mother_with_children_ta,
            all_qtr.single_parent_of_other_unknown_gender_with_children_ta,
            round(all_qtr.single_parent_of_other_unknown_gender_with_children_ta / all_qtr.households_in_ta, 2) AS perc_single_parent_of_other_unknown_gender_with_children_ta,
			all_qtr.single_man_ta,
			round(all_qtr.single_man_ta / all_qtr.households_in_ta, 2) AS perc_single_man_ta,
			all_qtr.single_woman_ta,
			round(all_qtr.single_woman_ta / all_qtr.households_in_ta, 2) AS perc_single_woman_ta,
			all_qtr.single_other_gender_ta,
			round(all_qtr.single_other_gender_ta / all_qtr.households_in_ta, 2) AS perc_single_other_gender_ta,
			all_qtr.all_other_household_types_ta,
			round(all_qtr.all_other_household_types_ta / all_qtr.households_in_ta, 2) AS perc_all_other_household_types_ta,
            all_qtr.quarter_ending,
            "left"(all_qtr.quarter_ending::text, 4)::integer AS qtr_year,
            "right"(all_qtr.quarter_ending::text, 2)::integer AS qtr_month
           FROM all_qtr
          ORDER BY all_qtr.quarter_ending, all_qtr.local_authority
        ), quarter_ind AS (
         SELECT summary.system_id,
            summary.local_authority,
            summary.households_in_ta,
            summary.couple_with_children_ta,
            summary.perc_couple_with_children_ta,
            summary.single_father_with_children_ta,
            summary.perc_single_father_with_children_ta,
            summary.single_mother_with_children_ta,
            summary.perc_single_mother_with_children_ta,
            summary.single_parent_of_other_unknown_gender_with_children_ta,
            summary.perc_single_parent_of_other_unknown_gender_with_children_ta,
			summary.single_man_ta,
			summary.single_woman_ta,
			summary.single_other_gender_ta,
			summary.all_other_household_types_ta,
			summary.perc_single_man_ta,
			summary.perc_single_woman_ta,
			summary.perc_single_other_gender_ta,
			summary.perc_all_other_household_types_ta,
            summary.quarter_ending,
            summary.qtr_year,
            summary.qtr_month,
                CASE
                    WHEN summary.qtr_month = 3 THEN 'q1'::text
                    WHEN summary.qtr_month = 6 THEN 'q2'::text
                    WHEN summary.qtr_month = 9 THEN 'q3'::text
                    WHEN summary.qtr_month = 12 THEN 'q4'::text
                    ELSE 'q0'::text
                END AS year_qtr
           FROM summary
        ), unpivot_table AS (
         SELECT quarter_ind.local_authority,
            quarter_ind.quarter_ending,
            quarter_ind.qtr_year,
            quarter_ind.year_qtr,
            quarter_ind.qtr_month,
            jsonb_each_text.key AS metrics,
            jsonb_each_text.value::numeric AS households
           FROM quarter_ind,
            LATERAL jsonb_each_text(to_jsonb(quarter_ind.*) - 'local_authority'::text - 'system_id'::text - 'quarter_ending'::text - 'qtr_year'::text - 'qtr_month'::text - 'year_qtr'::text) jsonb_each_text(key, value)
        ), pivot_la AS (
         SELECT unpivot_table.quarter_ending,
            unpivot_table.qtr_year,
            unpivot_table.year_qtr,
            unpivot_table.qtr_month,
            unpivot_table.metrics,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Hackney'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS hackney_la,
            max(
                CASE
                    WHEN unpivot_table.local_authority = 'Neighbouring LA'::text THEN unpivot_table.households
                    ELSE NULL::numeric
                END) AS neighbouring_la
           FROM unpivot_table
          GROUP BY unpivot_table.quarter_ending, unpivot_table.qtr_year, unpivot_table.year_qtr, unpivot_table.qtr_month, unpivot_table.metrics
          ORDER BY unpivot_table.quarter_ending
        )
 SELECT quarter_ending,
    qtr_year,
    year_qtr,
    qtr_month,
    metrics,
    hackney_la,
    neighbouring_la,
    hackney_la - neighbouring_la AS difference
   FROM pivot_la
WITH DATA;
	
SELECT *
FROM core.temp_accommodation_households_summary;