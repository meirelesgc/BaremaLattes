from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    Boolean,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, registry

table_registry = registry()


@table_registry.mapped_as_dataclass
class Country:
    __tablename__ = 'country'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    name_pt: Mapped[str] = mapped_column(String, unique=True)
    alpha_2_code: Mapped[Optional[str]] = mapped_column(
        String(2), unique=True, default=None
    )
    alpha_3_code: Mapped[Optional[str]] = mapped_column(
        String(3), unique=True, default=None
    )


@table_registry.mapped_as_dataclass
class State:
    __tablename__ = 'state'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    country_id: Mapped[UUID] = mapped_column(ForeignKey('country.id'))
    abbreviation: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )


@table_registry.mapped_as_dataclass
class City:
    __tablename__ = 'city'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    country_id: Mapped[UUID] = mapped_column(ForeignKey('country.id'))
    state_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('state.id'), default=None
    )


@table_registry.mapped_as_dataclass
class JCR:
    __tablename__ = 'jcr'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    rank: Mapped[Optional[str]] = mapped_column(String, default=None)
    journalname: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcryear: Mapped[Optional[str]] = mapped_column(String, default=None)
    abbrjournal: Mapped[Optional[str]] = mapped_column(String, default=None)
    issn: Mapped[Optional[str]] = mapped_column(String, default=None)
    eissn: Mapped[Optional[str]] = mapped_column(String, default=None)
    totalcites: Mapped[Optional[str]] = mapped_column(String, default=None)
    totalarticles: Mapped[Optional[str]] = mapped_column(String, default=None)
    citableitems: Mapped[Optional[str]] = mapped_column(String, default=None)
    citedhalflife: Mapped[Optional[str]] = mapped_column(String, default=None)
    citinghalflife: Mapped[Optional[str]] = mapped_column(String, default=None)
    jif2019: Mapped[Optional[float]] = mapped_column(default=None)
    url_revista: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class PeriodicalMagazine:
    __tablename__ = 'periodical_magazine'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[Optional[str]] = mapped_column(String, default=None)
    issn: Mapped[Optional[str]] = mapped_column(String, default=None)
    qualis: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcr: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcr_link: Mapped[Optional[str]] = mapped_column(String, default=None)
    reference_period: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ResearchGroup:
    __tablename__ = 'research_group'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[Optional[str]] = mapped_column(String, default=None)
    institution: Mapped[Optional[str]] = mapped_column(String, default=None)
    first_leader: Mapped[Optional[str]] = mapped_column(String, default=None)
    first_leader_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), default=None
    )
    second_leader: Mapped[Optional[str]] = mapped_column(String, default=None)
    second_leader_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), default=None
    )
    area: Mapped[Optional[str]] = mapped_column(String, default=None)
    census: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    start_of_collection: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    end_of_collection: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    group_identifier: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    institution_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    category: Mapped[Optional[str]] = mapped_column(String, default=None)

    __table_args__ = (
        UniqueConstraint(
            'name', 'institution', name='uq_research_group_name_institution'
        ),
    )


@table_registry.mapped_as_dataclass
class ResearchLines:
    __tablename__ = 'research_lines'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    research_group_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('research_group.id'), default=None
    )
    title: Mapped[Optional[str]] = mapped_column(Text, default=None)
    objective: Mapped[Optional[str]] = mapped_column(Text, default=None)
    keyword: Mapped[Optional[str]] = mapped_column(String, default=None)
    group_identifier: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    predominant_major_area: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    predominant_area: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class Institution:
    __tablename__ = 'institution'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    acronym: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    description: Mapped[Optional[str]] = mapped_column(String, default=None)
    lattes_id: Mapped[Optional[str]] = mapped_column(String, default=None)
    cnpj: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    image: Mapped[Optional[str]] = mapped_column(String, default=None)
    latitude: Mapped[Optional[float]] = mapped_column(Float, default=None)
    longitude: Mapped[Optional[float]] = mapped_column(Float, default=None)


@table_registry.mapped_as_dataclass
class GreatAreaExpertise:
    __tablename__ = 'great_area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)


@table_registry.mapped_as_dataclass
class AreaExpertise:
    __tablename__ = 'area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    great_area_expertise_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('great_area_expertise.id'),
        default=None,
    )


@table_registry.mapped_as_dataclass
class SubAreaExpertise:
    __tablename__ = 'sub_area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    area_expertise_id: Mapped[UUID] = mapped_column(
        ForeignKey('area_expertise.id')
    )


@table_registry.mapped_as_dataclass
class AreaSpecialty:
    __tablename__ = 'area_specialty'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    sub_area_expertise_id: Mapped[UUID] = mapped_column(
        ForeignKey('sub_area_expertise.id')
    )


@table_registry.mapped_as_dataclass
class Researcher:
    __tablename__ = 'researcher'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    lattes_id: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    lattes_10_id: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    last_update: Mapped[datetime] = mapped_column(
        server_default=text('now()'), init=False
    )
    citations: Mapped[Optional[str]] = mapped_column(String, default=None)
    orcid: Mapped[Optional[str]] = mapped_column(String, default=None)
    abstract: Mapped[Optional[str]] = mapped_column(Text, default=None)
    abstract_en: Mapped[Optional[str]] = mapped_column(Text, default=None)
    abstract_ai: Mapped[Optional[str]] = mapped_column(Text, default=None)
    other_information: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    city_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('city.id'), default=None
    )
    country_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('country.id'), default=None
    )
    qtt_publications: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    institution_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('institution.id'),
        default=None,
    )
    graduate_program: Mapped[Optional[str]] = mapped_column(String, default=None)
    graduation: Mapped[Optional[str]] = mapped_column(String, default=None)
    update_abstract: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=True
    )


@table_registry.mapped_as_dataclass
class ResearcherAddress:
    __tablename__ = 'researcher_address'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    city: Mapped[Optional[str]] = mapped_column(String, default=None)
    organ: Mapped[Optional[str]] = mapped_column(String, default=None)
    unity: Mapped[Optional[str]] = mapped_column(String, default=None)
    institution: Mapped[Optional[str]] = mapped_column(String, default=None)
    public_place: Mapped[Optional[str]] = mapped_column(String, default=None)
    district: Mapped[Optional[str]] = mapped_column(String, default=None)
    cep: Mapped[Optional[str]] = mapped_column(String, default=None)
    mailbox: Mapped[Optional[str]] = mapped_column(String, default=None)
    fax: Mapped[Optional[str]] = mapped_column(String, default=None)
    url_homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    telephone: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    uf: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ResearcherAreaExpertise:
    __tablename__ = 'researcher_area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    sub_area_expertise_id: Mapped[UUID] = mapped_column(
        ForeignKey('sub_area_expertise.id')
    )
    order: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    area_expertise_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('area_expertise.id'), default=None
    )
    great_area_expertise_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('great_area_expertise.id'), default=None
    )
    area_specialty_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('area_specialty.id'), default=None
    )


@table_registry.mapped_as_dataclass
class Education:
    __tablename__ = 'education'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    degree: Mapped[str] = mapped_column(String)
    education_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    education_start: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    education_end: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    key_words: Mapped[Optional[str]] = mapped_column(String, default=None)
    institution: Mapped[Optional[str]] = mapped_column(String, default=None)
    status: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class BibliographicProduction:
    __tablename__ = 'bibliographic_production'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    type: Mapped[str] = mapped_column(String)
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('country.id'), default=None
    )
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    means_divulgation: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    relevance: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    has_image: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    scientific_divulgation: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=False
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    authors: Mapped[Optional[str]] = mapped_column(String, default=None)
    year_: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)


@table_registry.mapped_as_dataclass
class BibliographicProductionArticle:
    __tablename__ = 'bibliographic_production_article'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    bibliographic_production_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id')
    )
    periodical_magazine_id: Mapped[UUID] = mapped_column(
        ForeignKey('periodical_magazine.id')
    )
    volume: Mapped[Optional[str]] = mapped_column(String, default=None)
    fascicle: Mapped[Optional[str]] = mapped_column(String, default=None)
    series: Mapped[Optional[str]] = mapped_column(String, default=None)
    start_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    end_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    place_publication: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    periodical_magazine_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    issn: Mapped[Optional[str]] = mapped_column(String, default=None)
    qualis: Mapped[Optional[str]] = mapped_column(String, default='SQ')
    jcr: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcr_link: Mapped[Optional[str]] = mapped_column(String, default=None)
    stars: Mapped[Optional[int]] = mapped_column(Integer, default=0)


@table_registry.mapped_as_dataclass
class BibliographicProductionBook:
    __tablename__ = 'bibliographic_production_book'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    bibliographic_production_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id')
    )
    isbn: Mapped[Optional[str]] = mapped_column(String, default=None)
    qtt_volume: Mapped[Optional[str]] = mapped_column(String, default=None)
    qtt_pages: Mapped[Optional[str]] = mapped_column(String, default=None)
    num_edition_revision: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    num_series: Mapped[Optional[str]] = mapped_column(String, default=None)
    publishing_company: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    publishing_company_city: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    stars: Mapped[Optional[int]] = mapped_column(Integer, default=0)


@table_registry.mapped_as_dataclass
class BibliographicProductionBookChapter:
    __tablename__ = 'bibliographic_production_book_chapter'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('uuid_generate_v4()'),
        init=False,
    )
    bibliographic_production_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id')
    )
    book_title: Mapped[Optional[str]] = mapped_column(String, default=None)
    isbn: Mapped[Optional[str]] = mapped_column(String, default=None)
    start_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    end_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    qtt_volume: Mapped[Optional[str]] = mapped_column(String, default=None)
    organizers: Mapped[Optional[str]] = mapped_column(String, default=None)
    num_edition_revision: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    num_series: Mapped[Optional[str]] = mapped_column(String, default=None)
    publishing_company: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    publishing_company_city: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    stars: Mapped[Optional[int]] = mapped_column(Integer, default=0)
