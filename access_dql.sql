-- View: public.initial_asmt_summary

DROP VIEW IF EXISTS public.initial_asmt_summary;

CREATE OR REPLACE VIEW public.initial_asmt_summary
 AS
 SELECT 
    quarter_ending,
    qtr_year,
    qtr_month,
    year_qtr,
	hackney_la,
	neighbouring_la,
    metrics,
	difference
   FROM core.initial_asmt_summary;

ALTER TABLE public.initial_asmt_summary
    OWNER TO postgres;

-- View: public.prevention_duty_summary

DROP VIEW IF EXISTS public.prevention_duty_summary;

CREATE OR REPLACE VIEW public.prevention_duty_summary
 AS
 SELECT 
    quarter_ending,
    qtr_year,
    qtr_month,
    year_qtr,
	hackney_la,
	neighbouring_la,
    metrics,
	difference
   FROM core.prevention_duty_summary;

ALTER TABLE public.prevention_duty_summary
    OWNER TO postgres;

-- View: public.relief_duty_summary

DROP VIEW IF EXISTS public.relief_duty_summary;

CREATE OR REPLACE VIEW public.relief_duty_summary
 AS
 SELECT 
    quarter_ending,
    qtr_year,
    qtr_month,
    year_qtr,
	hackney_la,
	neighbouring_la,
    metrics,
	difference
   FROM core.relief_duty_summary;

ALTER TABLE public.relief_duty_summary
    OWNER TO postgres;

-- View: public.prevention_duty_ending_summary

DROP VIEW IF EXISTS public.prevention_duty_ending_summary;

CREATE OR REPLACE VIEW public.prevention_duty_ending_summary
 AS
 SELECT 
    quarter_ending,
    qtr_year,
    qtr_month,
    year_qtr,
	hackney_la,
	neighbouring_la,
    metrics,
	difference
   FROM core.prevention_duty_ending_summary;

ALTER TABLE public.prevention_duty_ending_summary
    OWNER TO postgres;

-- View: public.relief_duty_ending_summary

DROP VIEW IF EXISTS public.relief_duty_ending_summary;

CREATE OR REPLACE VIEW public.relief_duty_ending_summary
 AS
 SELECT 
    quarter_ending,
    qtr_year,
    qtr_month,
    year_qtr,
	hackney_la,
	neighbouring_la,
    metrics,
	difference
   FROM core.relief_duty_ending_summary;

ALTER TABLE public.relief_duty_ending_summary
    OWNER TO postgres;

-- View: public.temp_accommodation_summary

DROP VIEW IF EXISTS public.temp_accommodation_summary;

CREATE OR REPLACE VIEW public.temp_accommodation_summary
 AS
 SELECT 
    quarter_ending,
    qtr_year,
    qtr_month,
    year_qtr,
	hackney_la,
	neighbouring_la,
    metrics,
	difference
   FROM core.temp_accommodation_summary;

ALTER TABLE public.temp_accommodation_summary
    OWNER TO postgres;

-- View: public.temp_accommodation_households_summary

DROP VIEW IF EXISTS public.temp_accommodation_households_summary;

CREATE OR REPLACE VIEW public.temp_accommodation_households_summary
 AS
 SELECT 
    quarter_ending,
    qtr_year,
    qtr_month,
    year_qtr,
	hackney_la,
	neighbouring_la,
    metrics,
	difference
   FROM core.temp_accommodation_households_summary;

ALTER TABLE public.temp_accommodation_households_summary
    OWNER TO postgres;
	

SELECT *
FROM public.initial_asmt_summary;