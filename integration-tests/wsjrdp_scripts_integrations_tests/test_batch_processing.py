from __future__ import annotations

import uuid

import pytest
import wsjrdp2027


class Test_DB_Updates:
    def add_marker_note(self, request, ctx: wsjrdp2027.WsjRdpContext, where):
        marker = str(uuid.uuid4())
        bc = wsjrdp2027.MailingConfig(
            name=request.node.name,
            where=where,
            updates={"add_note": "New note for {{ row.id }}" + " " + marker},
        )
        prepared_batch = ctx.load_people_and_prepare_mailing(bc)
        ctx.update_db_and_send_mailing(prepared_batch, zip_eml=False)
        return marker

    def test_add_note__check_notes_table(
        self, request: pytest.FixtureRequest, ctx: wsjrdp2027.WsjRdpContext
    ):
        import psycopg.rows
        from psycopg.sql import SQL

        marker = self.add_marker_note(request, ctx, wsjrdp2027.PeopleWhere(id=2))

        query = SQL("""SELECT * FROM "notes" WHERE "text" LIKE {}""").format(
            f"%{marker}"
        )
        with ctx.psycopg_connect() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        assert len(rows) == 1
        row = rows[0]
        assert row["subject_id"] == 2

    def test_add_note__check_note_list(
        self, request: pytest.FixtureRequest, ctx: wsjrdp2027.WsjRdpContext
    ):
        where = wsjrdp2027.PeopleWhere(id=2)
        marker = self.add_marker_note(request, ctx, where)

        with ctx.psycopg_connect() as conn:
            df = wsjrdp2027.load_people_dataframe(conn, where=where)

        assert len(df) == 1
        note_list = df.iloc[0]["note_list"]
        assert len(note_list) >= 1
        assert any(marker in note for note in note_list)

    def test_add_note__query_note_with_like_op(
        self, request: pytest.FixtureRequest, ctx: wsjrdp2027.WsjRdpContext
    ):
        where = wsjrdp2027.PeopleWhere(id=2)
        marker = self.add_marker_note(request, ctx, where)

        with ctx.psycopg_connect() as conn:
            df = wsjrdp2027.load_people_dataframe(
                conn,
                where=wsjrdp2027.PeopleWhere(note={"op": "like", "expr": f"%{marker}"}),
            )

        assert len(df) == 1
        row = df.iloc[0]
        assert row["id"] == where.id
        note_list = df.iloc[0]["note_list"]
        assert len(note_list) >= 1
        assert any(marker in note for note in note_list)
