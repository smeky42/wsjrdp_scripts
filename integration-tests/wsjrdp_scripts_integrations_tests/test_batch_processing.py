import uuid

import wsjrdp2027


class Test_DB_Updates:
    def test_new_note(self, ctx: wsjrdp2027.WsjRdpContext):
        import psycopg.rows
        from psycopg.sql import SQL

        marker = str(uuid.uuid4())
        bc = wsjrdp2027.MailingConfig(
            where=wsjrdp2027.PeopleWhere(id=65),
            updates={"new_note": "New note for {{ row.id }}" + marker},
        )
        prepared_batch = ctx.load_people_and_prepare_mailing(bc)
        ctx.update_db_and_send_mailing(prepared_batch, zip_eml=False)

        query = SQL("""SELECT * FROM "notes" WHERE "text" LIKE {}""").format(
            f"%{marker}"
        )
        with ctx.psycopg_connect() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        assert len(rows) == 1
        row = rows[0]
        assert row["subject_id"] == 65
