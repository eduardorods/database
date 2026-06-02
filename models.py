import uuid
from datetime import date
from decimal import Decimal

from pgvector.sqlalchemy import Vector
from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class CRIMetadata(Base):
    __tablename__ = "cri_metadata"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    codigo_if: Mapped[str] = mapped_column(String, index=True, nullable=False)
    securitizadora: Mapped[str | None] = mapped_column(String)
    numero_emissao: Mapped[int | None] = mapped_column(Integer)
    agente_fiduciario: Mapped[str | None] = mapped_column(String)
    devedor: Mapped[str | None] = mapped_column(String)
    data_emissao: Mapped[date | None] = mapped_column(Date)
    frequencia_juros: Mapped[str | None] = mapped_column(String)
    data_inicio_juros: Mapped[date | None] = mapped_column(Date)
    frequencia_amortizacao: Mapped[str | None] = mapped_column(String)

    series: Mapped[list["CRISerie"]] = relationship(
        back_populates="cri", cascade="all, delete-orphan"
    )
    documentos: Mapped[list["CRIDocumento"]] = relationship(
        back_populates="cri", cascade="all, delete-orphan"
    )
    clausulas: Mapped[list["CRIClausula"]] = relationship(
        back_populates="cri", cascade="all, delete-orphan"
    )


class CRISerie(Base):
    __tablename__ = "cri_serie"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    cri_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cri_metadata.id", ondelete="CASCADE"), nullable=False
    )
    nome_serie: Mapped[str | None] = mapped_column(String)
    data_vencimento: Mapped[date | None] = mapped_column(Date)
    indexador: Mapped[str | None] = mapped_column(String)
    taxa_spread: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))

    cri: Mapped["CRIMetadata"] = relationship(back_populates="series")


class CRIDocumento(Base):
    __tablename__ = "cri_documento"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    cri_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cri_metadata.id", ondelete="CASCADE"), nullable=False
    )
    caminho_drive: Mapped[str | None] = mapped_column(String)
    tipo_documento: Mapped[str | None] = mapped_column(String)

    cri: Mapped["CRIMetadata"] = relationship(back_populates="documentos")


class CRIClausula(Base):
    __tablename__ = "cri_clausula"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    cri_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("cri_metadata.id", ondelete="CASCADE"), nullable=False
    )
    tipo_clausula: Mapped[str | None] = mapped_column(String)
    texto_original: Mapped[str | None] = mapped_column(Text)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(768))

    cri: Mapped["CRIMetadata"] = relationship(back_populates="clausulas")
