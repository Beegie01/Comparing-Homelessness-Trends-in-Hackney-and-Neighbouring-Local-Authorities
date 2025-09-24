import tab_A1_etl as process_initial_assessment
import tab_A2P_etl as process_owed_prevention_duty
import tab_A2R_etl as process_owed_relief_duty
import tab_P1_etl as process_prevention_duty_ended
import tab_R1_etl as process_relief_duty_ended
import tab_TA1_etl as process_temp_accommodation
import tab_TA2_etl as process_temp_accommodation_households_composition

def app():

    sample_filename = "Detailed_LA_202406_revised.xlsx"

    process_initial_assessment.run_app(sample_filename)

    process_owed_prevention_duty.run_app(sample_filename)

    process_owed_relief_duty.run_app(sample_filename)

    process_prevention_duty_ended.run_app(sample_filename)

    process_relief_duty_ended.run_app(sample_filename)

    process_temp_accommodation.run_app(sample_filename)

    process_temp_accommodation_households_composition.run_app(sample_filename)

if __name__ == "__main__":
    app()