BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
create EXTENSION fuzzystrmatch;
create EXTENSION pg_trgm;
CREATE EXTENSION unaccent;
CREATE EXTENSION vector;

CREATE TYPE public.relationship AS ENUM ('COLABORADOR', 'PERMANENTE');
CREATE TYPE public.classification_class AS ENUM ('A+', 'A', 'B+', 'B', 'C+', 'C', 'D+', 'D', 'E+', 'E');
CREATE TYPE routine_type AS ENUM ('SOAP_LATTES', 'HOP', 'POPULATION', 'PRODUCTION', 'LATTES_10', 'IND_PROD', 'POG', 'OPEN_ALEX', 'SEARCH_TERM', 'STEP_RESEARCHER', 'STEP_PRODUCTION', 'STEP_ARTICLE', 'STEP_BOOK', 'STEP_TECHNICAL_PRODUCTION', 'STEP_GERAL_PRODUCTION');
CREATE TYPE public.bibliographic_production_type_enum AS ENUM ('BOOK', 'BOOK_CHAPTER', 'ARTICLE', 'WORK_IN_EVENT', 'TEXT_IN_NEWSPAPER_MAGAZINE');

CREATE SCHEMA IF NOT EXISTS embeddings;
CREATE SCHEMA IF NOT EXISTS ufmg;
CREATE SCHEMA IF NOT EXISTS logs;
 
-- CREATE TABLE IF NOT EXISTS public.country (
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     name character varying NOT NULL,
--     name_pt character varying NOT NULL,
--     alpha_2_code character(2),
--     alpha_3_code character(3),
--     created_at timestamp without time zone NOT NULL DEFAULT now(),
--     updated_at timestamp without time zone,
--     deleted_at timestamp without time zone,
--     CONSTRAINT "PK_bf6e37c231c4f4ea56dcd887269" PRIMARY KEY (id),
--     CONSTRAINT "UQ_2c5aa339240c0c3ae97fcc9dc4c" UNIQUE (name),
--     CONSTRAINT "UQ_69c6da9574151020d186279419f" UNIQUE (alpha_2_code),
--     CONSTRAINT "UQ_9f88595b715818e292be3472256" UNIQUE (alpha_3_code),
--     CONSTRAINT "UQ_f7c67d6e048708bb13b14a0bc1a" UNIQUE (name_pt)
-- );
-- CREATE TABLE IF NOT EXISTS public.state (
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     name character varying NOT NULL,
--     abbreviation character(2),
--     country_id uuid NOT NULL,
--     created_at timestamp without time zone NOT NULL DEFAULT now(),
--     updated_at timestamp without time zone,
--     deleted_at timestamp without time zone,
--     CONSTRAINT "PK_549ffd046ebab1336c3a8030a12" PRIMARY KEY (id),
--     CONSTRAINT "UQ_a4925b2350673eb963998d27ec3" UNIQUE (abbreviation),
--     CONSTRAINT "UQ_b2c4aef5929860729007ac32f6f" UNIQUE (name),
--     CONSTRAINT "FKCountryState" FOREIGN KEY (country_id) REFERENCES public.country (id) ON UPDATE CASCADE ON DELETE CASCADE
-- );
-- CREATE TABLE IF NOT EXISTS public.city (
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     name character varying NOT NULL,
--     country_id uuid NOT NULL,
--     state_id uuid,
--     created_at timestamp without time zone NOT NULL DEFAULT now(),
--     updated_at timestamp without time zone,
--     deleted_at timestamp without time zone,
--     CONSTRAINT "PK_b222f51ce26f7e5ca86944a6739" PRIMARY KEY (id),
--     CONSTRAINT "FKCountryCity" FOREIGN KEY (country_id) REFERENCES public.country (id) ON UPDATE CASCADE ON DELETE CASCADE,
--     CONSTRAINT "FKStateCity" FOREIGN KEY (state_id) REFERENCES public.state (id) ON UPDATE CASCADE ON DELETE CASCADE
-- );
-- CREATE TABLE IF NOT EXISTS public.institution (
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     name character varying NOT NULL,
--     acronym character varying(50),
--     description character varying(5000),
--     lattes_id character(12),
--     cnpj character(14),
--     image character varying,
--     created_at timestamp without time zone NOT NULL DEFAULT now(),
--     updated_at timestamp without time zone,
--     deleted_at timestamp without time zone,
--     latitude double precision,
--     longitude double precision,
--     CONSTRAINT "PK_f60ee4ff0719b7df54830b39087" PRIMARY KEY (id),
--     CONSTRAINT "UQ_c50c675ba2bedbaff7192b0a30e" UNIQUE (acronym),
--     CONSTRAINT "UQ_c9af99711dccbeb22b20b24cca8" UNIQUE (cnpj),
--     CONSTRAINT "UQ_d218ad3566afa9e396f184fd7d5" UNIQUE (name)
-- );
-- CREATE TABLE IF NOT EXISTS public.periodical_magazine (
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     name character varying(600),
--     issn character varying(20),
--     qualis character varying(8),
--     created_at timestamp without time zone NOT NULL DEFAULT now(),
--     updated_at timestamp without time zone,
--     deleted_at timestamp without time zone,
--     jcr character varying(100),
--     jcr_link character varying(200),
--     CONSTRAINT "PK_35bb0df687d8879d763c1f3ae68" PRIMARY KEY (id)
-- );
CREATE TABLE IF NOT EXISTS public.great_area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT pk_id_great_area_expertise PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS public.area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    great_area_expertise_id uuid,
    CONSTRAINT "PK_44d189c8477ad880b9ec101d453" PRIMARY KEY (id),
    CONSTRAINT "FK_great_area_expertise" FOREIGN KEY (great_area_expertise_id) REFERENCES public.great_area_expertise (id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.sub_area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    area_expertise_id uuid NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT pk_id_sub_area_expertise PRIMARY KEY (id),
    CONSTRAINT sub_area_expertise_area_expertise_id_fkey FOREIGN KEY (area_expertise_id) REFERENCES public.area_expertise (id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.area_specialty (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name character varying NOT NULL,
    sub_area_expertise_id uuid NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT pk_id_area_specialty PRIMARY KEY (id),
    CONSTRAINT area_specialty_sub_area_expertise_id_fkey FOREIGN KEY (sub_area_expertise_id) REFERENCES public.sub_area_expertise (id) ON UPDATE CASCADE ON DELETE CASCADE
);
-- CREATE TABLE IF NOT EXISTS public.researcher (
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     name character varying NOT NULL,
--     lattes_id character(16),
--     lattes_10_id character(10),
--     last_update timestamp without time zone NOT NULL DEFAULT now(),
--     citations character varying,
--     orcid character(31),
--     abstract TEXT,
--     abstract_en TEXT,
--     abstract_ai TEXT,
--     other_information character varying(5000),
--     city_id uuid,
--     country_id uuid,
--     classification classification_class DEFAULT 'E',
--     has_image boolean NOT NULL DEFAULT false,
--     qtt_publications integer,
--     institution_id uuid,
--     graduate_program character varying(255),
--     graduation character varying(30),
--     update_abstract boolean DEFAULT true,
--     docente boolean NOT NULL DEFAULT false,
--     student boolean NOT NULL DEFAULT false,
--     extra_field VARCHAR(255),
--     status boolean NOT NULL DEFAULT true,
--     stars INTEGER DEFAULT 0,
--     created_at timestamp without time zone NOT NULL DEFAULT now(),
--     updated_at timestamp without time zone,
--     deleted_at timestamp without time zone,
--     CONSTRAINT "PK_7b53850398061862ebe70d4ce44" PRIMARY KEY (id),
--     CONSTRAINT "UQ_cd7166a27f090d19d4e985592db" UNIQUE (lattes_10_id),
--     CONSTRAINT "UQ_fdf2bde0f46501e3e84ec154c32" UNIQUE (lattes_id),
--     CONSTRAINT "FKCityResearcher" FOREIGN KEY (city_id) REFERENCES public.city (id) ON UPDATE CASCADE ON DELETE CASCADE,
--     CONSTRAINT "FKCountryResearcher" FOREIGN KEY (country_id) REFERENCES public.country (id) ON UPDATE CASCADE ON DELETE CASCADE,
--     CONSTRAINT "FKInstitutionResearcher" FOREIGN KEY (institution_id) REFERENCES public.institution (id) ON UPDATE CASCADE ON DELETE CASCADE
-- );
-- CREATE TABLE IF NOT EXISTS public.researcher_address (
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     researcher_id uuid NOT NULL,
--     created_at timestamp without time zone NOT NULL DEFAULT now(),
--     updated_at timestamp without time zone,
--     deleted_at timestamp without time zone,
--     city character varying(50),
--     organ character varying(255),
--     unity character varying(255),
--     institution character varying(255),
--     public_place character varying(255),
--     district character varying(255),
--     cep character varying(255),
--     mailbox character varying(255),
--     fax character varying(20),
--     url_homepage character varying(300),
--     telephone character varying(20),
--     country character varying(100),
--     uf character varying(5),
--     CONSTRAINT "PK_180e58d987170694c2c11424916" PRIMARY KEY (id),
--     CONSTRAINT "FKAddressResearcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE CASCADE
-- );
CREATE TABLE IF NOT EXISTS public.researcher_area_expertise (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    sub_area_expertise_id uuid NOT NULL,
    "order" integer,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    area_expertise_id uuid,
    great_area_expertise_id uuid,
    area_specialty_id uuid,
    CONSTRAINT "PK_35338c2e178fa10e7b30966a4fc" PRIMARY KEY (id),
    CONSTRAINT "FKResearcherAreaExpertise" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FKSubAreaExpertise" FOREIGN KEY (sub_area_expertise_id) REFERENCES public.sub_area_expertise (id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FkAreaExpertise" FOREIGN KEY (area_expertise_id) REFERENCES public.area_expertise (id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FkAreaSpecialty" FOREIGN KEY (area_specialty_id) REFERENCES public.area_specialty (id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FkGreatAreaExpertise" FOREIGN KEY (great_area_expertise_id) REFERENCES public.great_area_expertise (id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.bibliographic_production (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    title character varying(500) NOT NULL,
    title_en character varying(500),
    type bibliographic_production_type_enum NOT NULL,
    doi character varying,
    nature character varying(50),
    year character(4),
    country_id uuid,
    language character(2),
    means_divulgation character varying(20),
    homepage character varying,
    relevance boolean NOT NULL DEFAULT false,
    has_image boolean NOT NULL DEFAULT false,
    scientific_divulgation boolean DEFAULT false,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    researcher_id uuid,
    authors character varying(1000),
    year_ integer,
    is_new boolean DEFAULT true,
    CONSTRAINT "PK_9c61219aee0513e9a1cf707a41a" PRIMARY KEY (id),
    CONSTRAINT "FKCountryResearcher" FOREIGN KEY (country_id) REFERENCES public.country (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.mockup (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_mockup_basic_data_id" PRIMARY KEY (id),
    CONSTRAINT "FK_mockup_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.publishing (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_publishing_id" PRIMARY KEY (id),
    CONSTRAINT "FK_publishing_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.industrial_design (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_industrial_design_id" PRIMARY KEY (id),
    CONSTRAINT "FK_industrial_design_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.maintenance_artistic_work (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_maintenance_artistic_work_id" PRIMARY KEY (id),
    CONSTRAINT "FK_maintenance_artistic_work_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.letter_map_or_similar (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_letter_map_or_similar_id" PRIMARY KEY (id),
    CONSTRAINT "FK_letter_map_or_similar_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.short_course (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_short_course_id" PRIMARY KEY (id),
    CONSTRAINT "FK_short_course_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.social_media_website_blog (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_social_media_website_blog_id" PRIMARY KEY (id),
    CONSTRAINT "FK_social_media_website_blog_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.other_technical_production (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid,
    production_sequence integer, 
    title character varying(500) NOT NULL,
    title_en character varying(500),
    year character(4),
    country character varying(100),
    language character varying(50),
    dissemination_medium character varying(50), 
    homepage character varying(500),
    doi character varying(100),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_other_technical_production_id" PRIMARY KEY (id),
    CONSTRAINT "FK_other_technical_production_researcher" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.advisory_activity (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    
    -- Dados extraídos do XML
    sequence_id integer,
    organ_code character varying(50),
    organ_name character varying(500) NOT NULL,
    unit_code character varying(50),
    unit_name character varying(255),
    specification text,
    
    -- Período da atividade
    is_current VARCHAR,
    start_month character(2),
    start_year character(4) NOT NULL,
    end_month character(2),
    end_year character(4),
    
    -- Timestamps para auditoria (padrão do modelo)
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    
    -- Constraints
    CONSTRAINT "PK_advisory_activity" PRIMARY KEY (id),
    CONSTRAINT "FK_advisory_activity_researcher" FOREIGN KEY (researcher_id) 
        REFERENCES public.researcher (id) MATCH SIMPLE 
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.software (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying,
    platform character varying,
    goal character varying,
    relevance boolean NOT NULL DEFAULT false,
    has_image boolean NOT NULL DEFAULT false,
    environment character varying,
    availability character varying,
    financing_institutionc character varying,
    researcher_id uuid,
    year smallint,
    is_new boolean DEFAULT true,
    stars INTEGER DEFAULT 0,
    CONSTRAINT software_pkey PRIMARY KEY (id),
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.patent (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(400),
    category character varying(200),
    relevance boolean NOT NULL DEFAULT false,
    has_image boolean NOT NULL DEFAULT false,
    development_year character varying(10),
    details TEXT,
    researcher_id uuid,
    code VARCHAR UNIQUE,
    grant_date timestamp without time zone,
    deposit_date character varying(255),
    is_new boolean DEFAULT true,
    stars INTEGER DEFAULT 0,
    CONSTRAINT patent_pkey PRIMARY KEY (id),
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.research_report (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    researcher_id uuid,
    title character varying(400),
    project_name character varying(255),
    financing_institutionc character varying(255),
    year smallint,
    is_new boolean DEFAULT true,
    stars INTEGER DEFAULT 0,
    CONSTRAINT research_report_pkey PRIMARY KEY (id),
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.guidance (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    researcher_id uuid,
    title character varying(400),
    nature character varying(255),
    oriented character varying(255),
    type character varying(255),
    status character varying(100),
    year smallint,
    is_new boolean DEFAULT true,
    stars INTEGER DEFAULT 0,
    CONSTRAINT guidance_pkey PRIMARY KEY (id),
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.brand (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(400),
    relevance boolean NOT NULL DEFAULT false,
    has_image boolean NOT NULL DEFAULT false,
    goal character varying(255),
    nature character varying(100),
    researcher_id uuid,
    year smallint,
    is_new boolean DEFAULT true,
    stars INTEGER DEFAULT 0,
    CONSTRAINT brand_pkey PRIMARY KEY (id),
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.participation_events (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(500),
    event_name character varying(500),
    nature character varying(30),
    form_participation character varying(30),
    type_participation character varying(30),
    researcher_id uuid,
    year smallint,
    is_new boolean DEFAULT true,
    stars INTEGER DEFAULT 0,
    CONSTRAINT participation_events_pkey PRIMARY KEY (id),
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.event_organization (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    title character varying(500),
    promoter_institution character varying(500),
    nature character varying(30),
    researcher_id uuid,
    local character varying(500),
    duration_in_weeks smallint,
    year smallint,
    is_new boolean DEFAULT true,
    CONSTRAINT event_organization_pkey PRIMARY KEY (id),
    CONSTRAINT fk_researcher_id FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production_article (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    bibliographic_production_id uuid NOT NULL,
    periodical_magazine_id uuid NOT NULL,
    volume character varying(30),
    fascicle character varying(30),
    series character varying(30),
    start_page character varying(30),
    end_page character varying(30),
    place_publication character varying,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    periodical_magazine_name character varying(600),
    issn character varying(20),
    qualis character varying(8) DEFAULT 'SQ',
    jcr character varying(100),
    jcr_link character varying(200),
    stars INTEGER DEFAULT 0,
    CONSTRAINT "PK_3a53ca9c0bd82c629e7a14ef0f4" PRIMARY KEY (id),
    CONSTRAINT "FKPeriodicalMagazineArticle" FOREIGN KEY (periodical_magazine_id) REFERENCES public.periodical_magazine (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "FKPublicationArticle" FOREIGN KEY (bibliographic_production_id) REFERENCES public.bibliographic_production (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production_work_in_event(
    bibliographic_production_id uuid NOT NULL UNIQUE,
    event_classification character varying(100),
    event_name character varying(600),
    event_city character varying(255),
    event_year integer,
    proceedings_title character varying(600),
    volume character varying(30),
    issue character varying(30),
    series character varying(100),
    start_page character varying(30),
    end_page character varying(30),
    publisher_name character varying(255),
    publisher_city character varying(255),
    event_name_english character varying(600),
    identifier_number character varying(100),
    isbn character varying(20),
    stars INTEGER DEFAULT 0,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT pk_bibliographic_production_event_work PRIMARY KEY (bibliographic_production_id),
    CONSTRAINT fk_bibliographic_production_event_work_production FOREIGN KEY (bibliographic_production_id) 
        REFERENCES public.bibliographic_production (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production_book (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    bibliographic_production_id uuid NOT NULL,
    isbn character(13),
    qtt_volume character varying(25),
    qtt_pages character varying(25),
    num_edition_revision character varying(25),
    num_series character varying(25),
    publishing_company character varying,
    publishing_company_city character varying,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    stars INTEGER DEFAULT 0,
    CONSTRAINT "PK_818a520edae9528a6d586485d18" PRIMARY KEY (id),
    CONSTRAINT "FKPublicationBook" FOREIGN KEY (bibliographic_production_id) REFERENCES public.bibliographic_production (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.bibliographic_production_book_chapter (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    bibliographic_production_id uuid NOT NULL,
    book_title character varying,
    isbn character(13),
    start_page character varying(25),
    end_page character varying(25),
    qtt_volume character varying(25),
    organizers character varying(500),
    num_edition_revision character varying(25),
    num_series character varying(25),
    publishing_company character varying,
    publishing_company_city character varying,
    stars INTEGER DEFAULT 0,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT "PK_ccc5964c28ffa1e316b8c0c821e" PRIMARY KEY (id),
    CONSTRAINT "FKPublicationBookChapter" FOREIGN KEY (bibliographic_production_id) REFERENCES public.bibliographic_production (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.research_dictionary (
    research_dictionary_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    term character varying(255),
    frequency integer DEFAULT 1,
    type_ character varying(30),
    CONSTRAINT research_dictionary_pkey PRIMARY KEY (research_dictionary_id),
    CONSTRAINT research_dictionary_term_type__key UNIQUE (term, type_)
);
CREATE TABLE IF NOT EXISTS public.graduate_program(
    graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    code VARCHAR(100),
    name VARCHAR(100) NOT NULL,
    name_en VARCHAR(100),
    basic_area VARCHAR(100), 
    cooperation_project VARCHAR(100),
    area VARCHAR(100) NOT NULL,
    modality VARCHAR(100) NOT NULL,
    TYPE VARCHAR(100) NULL,
    rating VARCHAR(5),
    institution_id uuid NOT NULL,
    state character varying(4) DEFAULT 'BA',
    city character varying(100) DEFAULT 'Salvador',
    region character varying(100) DEFAULT 'Nordeste',
    url_image VARCHAR(200) NULL,
    acronym character varying(100),
    description TEXT,
    visible bool DEFAULT FALSE,
    site TEXT,
    coordinator VARCHAR(100), 
    email VARCHAR(100),
    start DATE, 
    phone VARCHAR(255), 
    periodicity VARCHAR(50), 
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (graduate_program_id),
    FOREIGN KEY (institution_id) REFERENCES institution (id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.graduate_program_researcher(
    graduate_program_id uuid NOT NULL,
    researcher_id uuid NOT NULL,
    year INT[],
    type_ relationship,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (graduate_program_id, researcher_id),
    FOREIGN KEY (researcher_id) REFERENCES researcher (id) ON UPDATE  CASCADE ON DELETE CASCADE,
    FOREIGN KEY (graduate_program_id) REFERENCES graduate_program (graduate_program_id) ON UPDATE  CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.graduate_program_student(
    graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    year INT [],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (graduate_program_id, researcher_id, year),
    FOREIGN KEY (researcher_id) REFERENCES researcher (id) ON UPDATE  CASCADE ON DELETE CASCADE,
    FOREIGN KEY (graduate_program_id) REFERENCES graduate_program (graduate_program_id) ON UPDATE  CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.research_lines_programs (
    graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    area VARCHAR(255) NOT NULL,
    start_year INT,
    end_year INT,
    PRIMARY KEY (graduate_program_id, name),
    FOREIGN KEY (graduate_program_id) REFERENCES graduate_program (graduate_program_id) ON UPDATE  CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.JCR (
    rank character varying,
    journalName character varying,
    jcrYear character varying,
    abbrJournal character varying,
    issn character varying,
    eissn character varying,
    totalCites character varying,
    totalArticles character varying,
    citableItems character varying,
    citedHalfLife character varying,
    citingHalfLife character varying,
    jif2019 double precision,
    url_revista character varying
);

CREATE TABLE IF NOT EXISTS public.researcher_production (
    researcher_production_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    articles integer,
    book_chapters integer,
    book integer,
    work_in_event integer,
    patent integer,
    software integer,
    brand integer,
    great_area text,
    great_area_ text[],
    area_specialty text,
    city character varying(100),
    organ character varying(100),
    CONSTRAINT researcher_production_pkey PRIMARY KEY (researcher_production_id),
    CONSTRAINT researcher_production_researcher_id_fkey FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE  CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.foment (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid REFERENCES researcher(id) ON DELETE CASCADE ON UPDATE  CASCADE,
    modality_code character varying(50),
    modality_name character varying(255),
    call_title character varying(255),
    category_level_code character varying(50),
    funding_program_name character varying(255),
    institute_name character varying(255),
    aid_quantity integer,
    scholarship_quantity integer
);
CREATE TABLE education (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id UUID NOT NULL,
    degree VARCHAR(255) NOT NULL,
    education_name VARCHAR(255),
    education_start INTEGER,
    education_end INTEGER,
    key_words VARCHAR(255),
    institution VARCHAR(255),
    CONSTRAINT pk_education PRIMARY KEY (id),
    CONSTRAINT fk_researcher_education FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE  CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.openalex_article (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    article_id uuid NOT NULL,
    article_institution VARCHAR,
    issn VARCHAR,
    authors_institution VARCHAR,
    abstract TEXT,
    authors VARCHAR,
    language VARCHAR,
    citations_count SMALLINT,
    pdf VARCHAR,
    landing_page_url VARCHAR,
    keywords VARCHAR,
    CONSTRAINT "PK_FIXMEHELP" PRIMARY KEY (article_id)
);
CREATE TABLE IF NOT EXISTS public.openalex_researcher (
    researcher_id uuid UNIQUE,
    h_index integer,
    relevance_score double precision,
    works_count integer,
    cited_by_count integer,
    i10_index integer,
    scopus character varying(255),
    orcid character varying(255),
    openalex character varying(255),
    CONSTRAINT fk_researcher_op FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE  CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.researcher_ind_prod (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    year integer NOT NULL,
    ind_prod_article numeric(10, 3),
    ind_prod_book numeric(10, 3),
    ind_prod_book_chapter numeric(10, 3),
    ind_prod_software numeric(10, 3),
    ind_prod_report numeric(10, 3),
    ind_prod_granted_patent numeric(10, 3),
    ind_prod_not_granted_patent numeric(10, 3),
    ind_prod_guidance numeric(10, 3),
    CONSTRAINT "PKRIndProd" PRIMARY KEY (researcher_id, year),
    CONSTRAINT "FKRIndProd" FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE public.graduate_program_ind_prod (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    graduate_program_id uuid NOT NULL,
    year integer NOT NULL,
    ind_prod_article numeric(10, 3),
    ind_prod_book numeric(10, 3),
    ind_prod_book_chapter numeric(10, 3),
    ind_prod_software numeric(10, 3),
    ind_prod_report numeric(10, 3),
    ind_prod_granted_patent numeric(10, 3),
    ind_prod_not_granted_patent numeric(10, 3),
    ind_prod_guidance numeric(10, 3)
);
-- CREATE TABLE IF NOT EXISTS research_group (
--     id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
--     name character varying(200),
--     institution character varying(200),
--     first_leader character varying(200),
--     first_leader_id uuid,
--     second_leader character varying(200),
--     second_leader_id uuid,
--     area character varying(200),
--     census int,
--     start_of_collection character varying(200),
--     end_of_collection character varying(200),
--     group_identifier character varying(200),
--     year int,
--     institution_name character varying(200),
--     category character varying(200),
--     UNIQUE (name, institution),
--     UNIQUE (group_identifier)
-- );
CREATE TABLE IF NOT EXISTS research_group_researcher (
      research_group_id uuid NOT NULL,
      researcher_id uuid NOT NULL,

      PRIMARY KEY (research_group_id, researcher_id),
      FOREIGN KEY (researcher_id) REFERENCES public.researcher (id) ON DELETE CASCADE ON UPDATE CASCADE,
      FOREIGN KEY (research_group_id) REFERENCES research_group (id) ON DELETE CASCADE ON UPDATE CASCADE,

      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP
);
-- CREATE TABLE research_lines(
--     id uuid NOT NULL DEFAULT uuid_generate_v4(),
--     research_group_id uuid,
--     title TEXT,
--     objective TEXT,
--     keyword VARCHAR(510),
--     group_identifier VARCHAR(510),
--     year INT,
--     predominant_major_area VARCHAR(510),
--     predominant_area VARCHAR(510)
-- );
CREATE TABLE research_project (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE,
    start_year INT,
    end_year INT,
    agency_code VARCHAR(255),
    agency_name VARCHAR(255),
    project_name TEXT,
    status VARCHAR(255),
    nature VARCHAR(255),
    number_undergraduates INT DEFAULT 0,
    number_specialists INT DEFAULT 0,
    number_academic_masters INT DEFAULT 0,
    number_phd INT DEFAULT 0,
    description TEXT,
    stars INTEGER DEFAULT 0
);
CREATE TABLE research_project_components (
    project_id uuid NOT NULL REFERENCES public.research_project(id) ON UPDATE CASCADE ON DELETE CASCADE,
    name VARCHAR(255),
    lattes_id VARCHAR(255),
    citations VARCHAR
);
CREATE TABLE research_project_foment (
    project_id uuid NOT NULL REFERENCES public.research_project(id) ON UPDATE CASCADE ON DELETE CASCADE,
    agency_name VARCHAR(255),
    agency_code VARCHAR(255),
    nature VARCHAR(255)
);
CREATE TABLE research_project_production (
    project_id uuid NOT NULL REFERENCES public.research_project(id) ON UPDATE CASCADE ON DELETE CASCADE,
    title TEXT,
    type VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS public.technical_work (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
    funding_institution VARCHAR,
    duration INT,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.technical_work_presentation (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
    year INT,
	event_name VARCHAR,
	promoting_institution VARCHAR,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.technical_work_program (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
    year INT,
	theme VARCHAR,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.technological_product (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    country VARCHAR,
    title TEXT NOT NULL,
    nature VARCHAR,
	type VARCHAR,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.process_or_technique (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,

    -- Dados Básicos
    sequence_id integer,
    nature varchar(255),
    title text NOT NULL,
    title_en text,
    "year" character(4),
    country varchar(100),
    language varchar(50),
    dissemination_medium varchar(255),
    home_page text,
    doi varchar(255),
    is_relevant boolean DEFAULT false,
    has_innovation_potential VARCHAR,

    -- Detalhamento
    purpose text,
    purpose_en text,
    availability varchar(100),
    funding_institution varchar(500),
    city varchar(255),
    
    -- Controle
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    
    CONSTRAINT "PK_process_or_technique" PRIMARY KEY (id),
    CONSTRAINT "FK_process_or_technique_researcher" FOREIGN KEY (researcher_id)
        REFERENCES public.researcher (id) MATCH SIMPLE
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.process_author (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    process_id uuid NOT NULL,
    
    full_name varchar(500) NOT NULL,
    citation_name varchar(255),
    author_order integer,
    cnpq_id varchar(16),

    CONSTRAINT "PK_process_author" PRIMARY KEY (id),
    CONSTRAINT "FK_process_author_process" FOREIGN KEY (process_id)
        REFERENCES public.process_or_technique (id) MATCH SIMPLE
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.process_keyword (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    process_id uuid NOT NULL,
    
    keyword text NOT NULL,
    "order" smallint, -- Para manter a ordem original (PALAVRA-CHAVE-1, 2, etc.)

    CONSTRAINT "PK_process_keyword" PRIMARY KEY (id),
    CONSTRAINT "UQ_process_keyword_order" UNIQUE (process_id, keyword),
    CONSTRAINT "FK_process_keyword_process" FOREIGN KEY (process_id)
        REFERENCES public.process_or_technique (id) MATCH SIMPLE
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.process_knowledge_area (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    process_id uuid NOT NULL,
    
    major_area varchar(255),
    area_name varchar(255),
    sub_area_name varchar(255),
    specialty_name varchar(255),
    "order" smallint, -- Para manter a ordem original (AREA-DO-CONHECIMENTO-1, 2, etc.)

    CONSTRAINT "PK_process_knowledge_area" PRIMARY KEY (id),
    CONSTRAINT "FK_process_knowledge_area_process" FOREIGN KEY (process_id)
        REFERENCES public.process_or_technique (id) MATCH SIMPLE
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.didactic_material (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    title TEXT NOT NULL,
    country VARCHAR,
    nature VARCHAR,
	description TEXT,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.artistic_production (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL,
    title TEXT NOT NULL,
    year INT,
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id) REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.relevant_production (
    researcher_id uuid NOT NULL,
    production_id uuid NOT NULL,
    type varchar NOT NULL,
    has_image boolean NOT NULL DEFAULT false,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (researcher_id, production_id, type),
    CONSTRAINT fk_researcher
        FOREIGN KEY (researcher_id)
        REFERENCES public.researcher(id)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.researcher_professional_experience (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    researcher_id uuid NOT NULL REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE, 
    enterprise VARCHAR(255),
    start_year INT, 
    end_year INT, 
    employment_type VARCHAR(255),
    other_employment_type VARCHAR(255),
    functional_classification VARCHAR(255),
    other_functional_classification VARCHAR(255),
    workload_hours_weekly VARCHAR(255),
    exclusive_dedication BOOLEAN,
    additional_info TEXT
);
CREATE TABLE public.labs (
    id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    hashed_id VARCHAR(32) NOT NULL,
    type VARCHAR(100) NOT NULL,
    location TEXT,
    name TEXT NOT NULL,
    description TEXT,
    website TEXT,
    activities TEXT,
    areas TEXT,
    campus TEXT,
    institution_id UUID,
    researcher_id UUID,
    responsible TEXT,
    FOREIGN KEY (researcher_id) REFERENCES researcher (id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (institution_id) REFERENCES institution (id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS logs.routine (
    type VARCHAR NOT NULL,
    error BOOLEAN DEFAULT FALSE,
    detail TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS ufmg.departament (
    dep_id VARCHAR(255),
    org_cod VARCHAR(255),
    dep_nom VARCHAR(255),
    dep_des TEXT,
    dep_email VARCHAR(255),
    dep_site TEXT,
    dep_sigla VARCHAR(255),
    dep_tel VARCHAR(255),
    PRIMARY KEY (dep_id)
);
CREATE TABLE IF NOT EXISTS ufmg.researcher (
    researcher_id UUID PRIMARY KEY REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE,
    
    -- Campos comuns
    full_name VARCHAR(255),
    gender VARCHAR(255),
    status_code VARCHAR(255),
    work_regime VARCHAR(255),
    job_class CHAR,
    job_title VARCHAR(255),
    job_rank VARCHAR(255),
    job_reference_code VARCHAR(255),
    academic_degree VARCHAR(255),
    organization_entry_date DATE,
    last_promotion_date DATE,
    
    -- Novos campos
    employment_status_description VARCHAR(255),
    department_name VARCHAR(255),
    career_category VARCHAR(255),
    academic_unit VARCHAR(255),
    unit_code VARCHAR(255),
    function_code VARCHAR(255),
    position_code VARCHAR(255),
    leadership_start_date DATE,
    leadership_end_date DATE,
    current_function_name VARCHAR(255),
    function_location VARCHAR(255),
    
    -- Campos que estavam só na tabela antiga
    registration_number VARCHAR(200),           
    ufmg_registration_number VARCHAR(200),      
    semester_reference VARCHAR(6)               
);
CREATE TABLE IF NOT EXISTS ufmg.technician (
    technician_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Campos comuns
    full_name VARCHAR(255),
    gender VARCHAR(255),
    status_code VARCHAR(255),
    work_regime VARCHAR(255),
    job_class CHAR,
    job_title VARCHAR(255),
    job_rank VARCHAR(255),
    job_reference_code VARCHAR(255),
    academic_degree VARCHAR(255),
    organization_entry_date DATE,
    last_promotion_date DATE,

    -- Novos campos
    employment_status_description VARCHAR(255),
    department_name VARCHAR(255),
    career_category VARCHAR(255),
    academic_unit VARCHAR(255),
    unit_code VARCHAR(255),
    function_code VARCHAR(255),
    position_code VARCHAR(255),
    leadership_start_date DATE,
    leadership_end_date DATE,
    current_function_name VARCHAR(255),
    function_location VARCHAR(255),

    -- Campos antigos
    registration_number VARCHAR(255),
    ufmg_registration_number VARCHAR(255),
    semester_reference VARCHAR(6)
);
CREATE TABLE IF NOT EXISTS ufmg.departament_technician (
    dep_id character varying(255),
    technician_id uuid,
    PRIMARY KEY (dep_id, technician_id),
    FOREIGN KEY (dep_id) REFERENCES ufmg.departament (dep_id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (technician_id) REFERENCES ufmg.technician (technician_id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS ufmg.departament_researcher (
    dep_id VARCHAR(20),
    researcher_id uuid NOT NULL,
    PRIMARY KEY (dep_id, researcher_id),
    FOREIGN KEY (dep_id) REFERENCES ufmg.departament (dep_id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (researcher_id) REFERENCES researcher (id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS ufmg.researcher_data(
    nome VARCHAR(255),
    cpf VARCHAR(14),
    classe INT,
    nivel INT,
    inicio TIMESTAMP,
    fim TIMESTAMP,
    tempo_nivel INT,
    tempo_acumulado INT,
    arquivo VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS ufmg.mandate(
    member VARCHAR(255),
    departament VARCHAR(255),
    mandate VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS embeddings.abstract (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.researcher(id) ON UPDATE CASCADE ON DELETE CASCADE,
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.article (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.bibliographic_production(id) ON UPDATE CASCADE ON DELETE CASCADE, 
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.article_abstract (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.openalex_article(article_id) ON UPDATE CASCADE ON DELETE CASCADE,
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.book (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.bibliographic_production(id) ON UPDATE CASCADE ON DELETE CASCADE,
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.event (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.bibliographic_production(id) ON UPDATE CASCADE ON DELETE CASCADE,
    embeddings vector,
    price numeric(20, 18)
);
CREATE TABLE IF NOT EXISTS embeddings.patent (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    reference_id uuid REFERENCES public.patent(id) ON UPDATE CASCADE ON DELETE CASCADE,
    embeddings vector,
    price numeric(20, 18)
);

CREATE INDEX ON researcher USING gin (name gin_trgm_ops);
CREATE INDEX ON researcher USING gin (abstract gin_trgm_ops);
CREATE INDEX ON researcher USING gin (abstract_en gin_trgm_ops);

CREATE INDEX ON great_area_expertise USING gin (name gin_trgm_ops);
CREATE INDEX ON area_expertise USING gin (name gin_trgm_ops);
CREATE INDEX ON periodical_magazine USING gin (name gin_trgm_ops);

CREATE INDEX ON bibliographic_production USING gin (title gin_trgm_ops);
CREATE INDEX ON brand USING gin (title gin_trgm_ops);
CREATE INDEX ON software USING gin (title gin_trgm_ops);
CREATE INDEX ON event_organization USING gin (title gin_trgm_ops);

--- admin
-- Criação dos novos esquemas
CREATE SCHEMA IF NOT EXISTS admin;
CREATE SCHEMA IF NOT EXISTS admin_ufmg;

-- Tabela Institution no esquema admin
CREATE TABLE IF NOT EXISTS admin.institution(
    institution_id uuid DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    acronym VARCHAR(50) UNIQUE,
    lattes_id CHAR(16),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (institution_id)
);

-- Tabela Researcher no esquema admin
CREATE TABLE IF NOT EXISTS admin.researcher(
    researcher_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name VARCHAR(150) NOT NULL,
    lattes_id VARCHAR(20),
    extra_field VARCHAR(255),
    status BOOL NOT NULL DEFAULT True,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (researcher_id),
    UNIQUE (lattes_id)
);

-- Tabela de Junção Researcher_Institution no esquema admin
CREATE TABLE IF NOT EXISTS admin.researcher_institution(
    researcher_institution_id uuid DEFAULT uuid_generate_v4(),
    researcher_id uuid NOT NULL,
    institution_id uuid NOT NULL,
    start_date DATE DEFAULT CURRENT_DATE, -- Opcional: Data de início do vínculo
    end_date DATE, -- Opcional: Data de fim do vínculo
    is_current BOOLEAN DEFAULT TRUE, -- Opcional: Indica se é o vínculo atual
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (researcher_institution_id),
    FOREIGN KEY (researcher_id) REFERENCES admin.researcher (researcher_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (institution_id) REFERENCES admin.institution (institution_id) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (researcher_id, institution_id, is_current) -- Garante que um pesquisador só tenha um vínculo atual com uma instituição
);
-- Tabela Graduate_Program no esquema admin
CREATE TABLE IF NOT EXISTS admin.graduate_program(
      graduate_program_id uuid NOT NULL DEFAULT uuid_generate_v4(),
      code VARCHAR(100) UNIQUE,
      name VARCHAR(100) NOT NULL,
      area VARCHAR(100) NOT NULL,
      modality VARCHAR(100) NOT NULL,
      TYPE VARCHAR(100) NULL,
      rating VARCHAR(5),
      institution_id uuid NOT NULL,
      state character varying(4) DEFAULT 'BA',
      city character varying(100) DEFAULT 'Salvador',
      region character varying(100) DEFAULT 'Nordeste',
      url_image VARCHAR(200) NULL,
      acronym character varying(100),
      description TEXT,
      visible bool DEFAULT FALSE,
      site TEXT,
      menagers TEXT[],
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (graduate_program_id),
      FOREIGN KEY (institution_id) REFERENCES admin.institution (institution_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS admin.graduate_program_researcher(
      graduate_program_id uuid NOT NULL,
      researcher_id uuid NOT NULL,
      year INT [],
      type_ relationship,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (graduate_program_id, researcher_id),
      FOREIGN KEY (researcher_id) REFERENCES admin.researcher (researcher_id) ON DELETE CASCADE ON UPDATE CASCADE,
      FOREIGN KEY (graduate_program_id) REFERENCES admin.graduate_program (graduate_program_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE IF NOT EXISTS admin.graduate_program_student(
      graduate_program_id uuid NOT NULL,
      researcher_id uuid NOT NULL,
      year INT [],
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (graduate_program_id, researcher_id, year),
      FOREIGN KEY (researcher_id) REFERENCES admin.researcher (researcher_id) ON DELETE CASCADE ON UPDATE CASCADE,
      FOREIGN KEY (graduate_program_id) REFERENCES admin.graduate_program (graduate_program_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabela Weights no esquema admin
CREATE TABLE IF NOT EXISTS admin.weights (
      institution_id uuid PRIMARY KEY,
      a1 numeric(20, 3),
      a2 numeric(20, 3),
      a3 numeric(20, 3),
      a4 numeric(20, 3),
      b1 numeric(20, 3),
      b2 numeric(20, 3),
      b3 numeric(20, 3),
      b4 numeric(20, 3),
      c numeric(20, 3),
      sq numeric(20, 3),
      book numeric(20, 3),
      book_chapter numeric(20, 3),
      software character varying,
      patent_granted character varying,
      patent_not_granted character varying,
      report character varying,
      f1 numeric(20, 3) DEFAULT 0,
      f2 numeric(20, 3) DEFAULT 0,
      f3 numeric(20, 3) DEFAULT 0,
      f4 numeric(20, 3) DEFAULT 0,
      f5 numeric(20, 3) DEFAULT 0,
      FOREIGN KEY (institution_id) REFERENCES admin.institution (institution_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabela Roles no esquema admin
CREATE TABLE IF NOT EXISTS admin.roles (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      role VARCHAR(255) NOT NULL UNIQUE
);

-- Tabela Permission no esquema admin
CREATE TABLE IF NOT EXISTS admin.permission (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      role_id UUID NOT NULL REFERENCES admin.roles(id) ON DELETE CASCADE ON UPDATE CASCADE,
      permission VARCHAR(255) NOT NULL,
      UNIQUE (role_id, permission)
);

-- Tabela Users no esquema admin
CREATE TABLE IF NOT EXISTS admin.users (
    user_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    display_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    uid VARCHAR(255) UNIQUE NOT NULL,
    photo_url TEXT,
    lattes_id VARCHAR(255),
    institution_id uuid,
    provider VARCHAR(255),
    linkedin VARCHAR(255),
    verify bool DEFAULT FALSE,
    shib_id VARCHAR(255),
    shib_code VARCHAR(255),
    birth_date VARCHAR(10),
    course_level VARCHAR(255),
    first_name VARCHAR(255),
    registration VARCHAR(255),
    gender VARCHAR(50),
    last_name VARCHAR(255),
    email_status VARCHAR(50),
    visible_email BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    FOREIGN KEY (institution_id) REFERENCES admin.institution (institution_id) ON DELETE SET NULL ON UPDATE CASCADE
);

-- Tabela Users_Roles no esquema admin
CREATE TABLE IF NOT EXISTS admin.users_roles (
      role_id UUID NOT NULL,
      user_id UUID NOT NULL,
      PRIMARY KEY (role_id, user_id),
      FOREIGN KEY (user_id) REFERENCES admin.users (user_id) ON DELETE CASCADE ON UPDATE CASCADE,
      FOREIGN KEY (role_id) REFERENCES admin.roles (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabela Newsletter_Subscribers no esquema admin
CREATE TABLE IF NOT EXISTS admin.newsletter_subscribers (
      id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
      email VARCHAR(255) NOT NULL UNIQUE,
      subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela Feedback no esquema admin
CREATE TABLE IF NOT EXISTS admin.feedback (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    rating INTEGER CHECK (rating >= 0 AND rating <= 10) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--- SCHEMA admin_ufmg ---

-- Tabela admin_ufmg.Researcher
CREATE TABLE IF NOT EXISTS admin_ufmg.researcher (
    researcher_id UUID PRIMARY KEY REFERENCES admin.researcher(researcher_id) ON DELETE CASCADE ON UPDATE CASCADE,

    -- Campos comuns
    full_name VARCHAR(255),
    gender VARCHAR(255),
    status_code VARCHAR(255),
    work_regime VARCHAR(255),
    job_class CHAR(25),
    job_title VARCHAR(255),
    job_rank VARCHAR(255),
    job_reference_code VARCHAR(255),
    academic_degree VARCHAR(255),
    organization_entry_date DATE,
    last_promotion_date DATE,

    -- Novos campos
    employment_status_description VARCHAR(255),
    department_name VARCHAR(255),
    career_category VARCHAR(255),
    academic_unit VARCHAR(255),
    unit_code VARCHAR(255),
    function_code VARCHAR(255),
    position_code VARCHAR(255),
    leadership_start_date DATE,
    leadership_end_date DATE,
    current_function_name VARCHAR(255),
    function_location VARCHAR(255),

    -- Campos que estavam só na tabela antiga
    registration_number VARCHAR(200),
    ufmg_registration_number VARCHAR(200),
    semester_reference VARCHAR(6)
);

-- Tabela admin_ufmg.Technician
CREATE TABLE IF NOT EXISTS admin_ufmg.technician (
    technician_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Campos comuns
    full_name VARCHAR(255),
    gender VARCHAR(255),
    status_code VARCHAR(255),
    work_regime VARCHAR(255),
    job_class CHAR(25),
    job_title VARCHAR(255),
    job_rank VARCHAR(255),
    job_reference_code VARCHAR(255),
    academic_degree VARCHAR(255),
    organization_entry_date DATE,
    last_promotion_date DATE,

    -- Novos campos
    employment_status_description VARCHAR(255),
    department_name VARCHAR(255),
    career_category VARCHAR(255),
    academic_unit VARCHAR(255),
    unit_code VARCHAR(255),
    function_code VARCHAR(255),
    position_code VARCHAR(255),
    leadership_start_date DATE,
    leadership_end_date DATE,
    current_function_name VARCHAR(255),
    function_location VARCHAR(255),

    -- Campos antigos
    registration_number VARCHAR(255),
    ufmg_registration_number VARCHAR(255),
    semester_reference VARCHAR(6)
);

-- Tabela admin_ufmg.Department
CREATE TABLE IF NOT EXISTS admin_ufmg.department (
      dep_id VARCHAR(20) PRIMARY KEY,
      org_cod VARCHAR(3),
      dep_nom VARCHAR(100),
      dep_des TEXT,
      dep_email VARCHAR(100),
      dep_site VARCHAR(100),
      dep_sigla VARCHAR(30),
      dep_tel VARCHAR(20),
      img_data BYTEA
);

-- Tabela admin_ufmg.Department_Technician
CREATE TABLE IF NOT EXISTS admin_ufmg.department_technician (
      dep_id VARCHAR(20),
      technician_id uuid,
      PRIMARY KEY (dep_id, technician_id),
      FOREIGN KEY (dep_id) REFERENCES admin_ufmg.department (dep_id) ON DELETE CASCADE ON UPDATE CASCADE,
      FOREIGN KEY (technician_id) REFERENCES admin_ufmg.technician (technician_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabela admin_ufmg.Department_Researcher
CREATE TABLE IF NOT EXISTS admin_ufmg.department_researcher (
      dep_id VARCHAR(20),
      researcher_id uuid NOT NULL,
      PRIMARY KEY (dep_id, researcher_id),
      FOREIGN KEY (dep_id) REFERENCES admin_ufmg.department (dep_id) ON DELETE CASCADE ON UPDATE CASCADE,
      FOREIGN KEY (researcher_id) REFERENCES admin.researcher (researcher_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabela admin_ufmg.Disciplines
CREATE TABLE IF NOT EXISTS admin_ufmg.disciplines (
      discipline_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      dep_id VARCHAR(20),
      semester VARCHAR(20),
      department VARCHAR(255),
      academic_activity_code VARCHAR(255),
      academic_activity_name VARCHAR(255),
      academic_activity_ch VARCHAR(255),
      demanding_courses VARCHAR(255),
      oft VARCHAR(50),
      available_slots VARCHAR(50),
      occupied_slots VARCHAR(50),
      percent_occupied_slots VARCHAR(50),
      schedule VARCHAR(255),
      language VARCHAR(50),
      researcher_id uuid [],
      researcher_name VARCHAR [],
      status VARCHAR(50),
      workload VARCHAR [],
      FOREIGN KEY (dep_id) REFERENCES admin_ufmg.department (dep_id) ON DELETE SET NULL ON UPDATE CASCADE
);

COMMIT;

ROLLBACK;
