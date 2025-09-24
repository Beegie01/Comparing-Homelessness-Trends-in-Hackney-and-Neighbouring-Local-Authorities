DROP SCHEMA IF EXISTS staging CASCADE;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.tab_a1 (
	system_id varchar(20) NOT NULL,
	local_authority varchar(50) NOT NULL,
	initial_assessments INTEGER,
	owed_prevention_or_relief_duty INTEGER,
	prevention_duty_owed INTEGER,
	relief_duty_owed INTEGER,
	households_in_area_000s NUMERIC,
	quarter_ending INTEGER,
	is_neighbouring_la INTEGER
);

CREATE TABLE staging.tab_a2p (
	system_id varchar(20) NOT NULL,
	local_authority varchar(50) NOT NULL,
	prevention_duty_owed INTEGER,
	family_or_friend_terminations INTEGER,
	ast_private_rented_terminations INTEGER,
	domestic_abuse_terminations INTEGER,
	non_violent_relationship_breakdown_terminations INTEGER,
	social_rented_tenancy_terminations INTEGER,
	supported_housing_terminations INTEGER,
	non_ast_private_rented_terminations INTEGER,
	other_violence_or_harassment_terminations INTEGER,
	institution_departures INTEGER,
	home_office_asylum_support_terminations INTEGER,
	new_home_for_illness_or_disability INTEGER,
	loss_of_placement_or_sponsorship INTEGER,
	for_other_or_unknown_reasons INTEGER,
	quarter_ending INTEGER,
	is_neighbouring_la INTEGER
);

CREATE TABLE staging.tab_a2r (
	system_id varchar(20) NOT NULL,
	local_authority varchar(50) NOT NULL,
	relief_duty_owed INTEGER,
	family_or_friend_terminations INTEGER,
	ast_private_rented_terminations INTEGER,
	domestic_abuse_terminations INTEGER,
	non_violent_relationship_breakdown_terminations INTEGER,
	social_rented_tenancy_terminations INTEGER,
	supported_housing_terminations INTEGER,
	non_ast_private_rented_terminations INTEGER,
	other_violence_or_harassment_terminations INTEGER,
	institution_departures INTEGER,
	home_office_asylum_support_terminations INTEGER,
	new_home_for_illness_or_disability INTEGER,
	loss_of_placement_or_sponsorship INTEGER,
	for_other_or_unknown_reasons INTEGER,
	quarter_ending INTEGER,
	is_neighbouring_la INTEGER
);

CREATE TABLE staging.tab_p1 (
	system_id varchar(20) NOT NULL, 
	local_authority varchar(50) NOT NULL, 
	prevention_duty_ended INTEGER, 
	secured_accommodation INTEGER, 
	homelessness INTEGER, 
	contact_lost INTEGER, 
	no_further_action_after_56days INTEGER, 
	applicant_withdrew_or_deceased INTEGER, 
	no_longer_eligible INTEGER, 
	rejected_offered_accommodation INTEGER, 
	uncooperative INTEGER, 
	not_known INTEGER, 
	quarter_ending INTEGER,
	is_neighbouring_la INTEGER
);

CREATE TABLE staging.tab_r1 (
	system_id varchar(20) NOT NULL, 
	local_authority varchar(50) NOT NULL,
	relief_duty_ended INTEGER, 
	secured_accommodation INTEGER,
	after_56days_deadline INTEGER, 
	contact_lost INTEGER, 
	applicant_withdrew_or_deceased INTEGER, 
	rejected_final_accommodation_offered INTEGER, 
	intentionally_homeless_from_accommodation_provided INTEGER, 
	accepted_by_another_la INTEGER, 
	no_longer_eligible INTEGER, 
	uncooperative_and_served_notice INTEGER, 
	not_known INTEGER, 
	quarter_ending INTEGER,
	is_neighbouring_la INTEGER
);

CREATE TABLE staging.tab_ta1 (
	system_id varchar(20) NOT NULL, 
	local_authority varchar(50) NOT NULL,
	households_in_ta INTEGER, 
	ta_households_with_children INTEGER, 
	children_headcount_in_ta INTEGER, 
	bnb_ta_households INTEGER, 
	bnb_ta_with_children INTEGER, 
	bnb_ta_with_children_exceeding_6wks INTEGER, 
	bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal INTEGER, 
	bnb_ta_with_16yo_17yo_main_applicant INTEGER, 
	nightly_paid_ta_households INTEGER, 
	nightly_paid_ta_with_children INTEGER, 
	hostel_ta_households INTEGER, 
	hostel_ta_with_children INTEGER, 
	private_sector_ta INTEGER, 
	private_sector_ta_with_children INTEGER, 
	la_ha_owned_managed_ta_households INTEGER, 
	la_ha_owned_managed_ta_with_children INTEGER, 
	any_other_type_ta INTEGER, 
	any_other_type_ta_with_children INTEGER, 
	in_another_la_ta INTEGER, 
	no_secured_accommodation_ta INTEGER, 
	no_secured_accommodation_ta_with_children INTEGER, 
	quarter_ending INTEGER,
	is_neighbouring_la INTEGER
);

CREATE TABLE staging.tab_ta2 (
	system_id varchar(20) NOT NULL, 
	local_authority varchar(50) NOT NULL,
	households_in_ta INTEGER,
	couple_with_children_ta INTEGER,
	single_father_with_children_ta INTEGER,
	single_mother_with_children_ta INTEGER,
	single_parent_of_other_unknown_gender_with_children_ta INTEGER,
	single_man_ta INTEGER,
	single_woman_ta INTEGER,
	single_other_gender_ta INTEGER,
	all_other_household_types_ta INTEGER,
	quarter_ending INTEGER,
	is_neighbouring_la INTEGER
);