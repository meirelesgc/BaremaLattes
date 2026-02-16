from typing import Optional
from uuid import UUID

from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint, text
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
