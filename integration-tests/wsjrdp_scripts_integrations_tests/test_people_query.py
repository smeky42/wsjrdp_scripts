from __future__ import annotations

import wsjrdp2027


_MAILING_COLS = [
    "mailing_from",
    "mailing_to",
    "mailing_cc",
    "mailing_bcc",
    "mailing_reply_to",
    "sepa_mailing_from",
    "sepa_mailing_to",
    "sepa_mailing_cc",
    "sepa_mailing_bcc",
    "sepa_mailing_reply_to",
]


class Test_Query_Tags:
    def test__query_people_with_warteliste_like_tag(
        self, ctx: wsjrdp2027.WsjRdpContext
    ):
        where = wsjrdp2027.PeopleWhere(tag={"op": "ilike", "expr": "%Warteliste%"})
        with ctx.psycopg_connect() as conn:
            df = wsjrdp2027.load_people_dataframe(conn, where=where)

        tag_set = set()
        for _, row in df.iterrows():
            tag_list = row["tag_list"]
            assert any("warteliste" in tag.lower() for tag in tag_list)
            tag_set.update(tag for tag in tag_list if "warteliste" in tag.lower())

        tags = sorted(tag_set)
        print(tags)
        assert len(tags) >= 2

    @staticmethod
    def __load_mailing_dicts(ctx: wsjrdp2027.WsjRdpContext, *queries) -> list[dict]:
        results = []
        with ctx.psycopg_connect() as conn:
            for query in queries:
                df = wsjrdp2027.load_people_dataframe(conn, query=query)
                assert len(df) == 1
                row = df.iloc[0]
                results.append({k: row.get(k) for k in _MAILING_COLS})
        return results

    def test__query_people__with_and_without__include_sepa_mail_in_mailing_to(
        self, ctx: wsjrdp2027.WsjRdpContext
    ):
        where = wsjrdp2027.PeopleWhere(id=182)
        query_t = wsjrdp2027.PeopleQuery(
            where=where, include_sepa_mail_in_mailing_to=True
        )
        query_f = wsjrdp2027.PeopleQuery(
            where=where, include_sepa_mail_in_mailing_to=False
        )
        query_n = wsjrdp2027.PeopleQuery(where=where)

        m_t, m_f, m_n = self.__load_mailing_dicts(ctx, query_t, query_f, query_n)

        assert m_f == m_n

        assert m_t["mailing_to"] is not None
        assert m_f["mailing_to"] is not None
        assert m_t["mailing_to"] != m_f["mailing_to"]
        assert m_f["mailing_to"] == m_n["mailing_to"]
        assert len(m_t["mailing_to"]) > len(m_f["mailing_to"])

        all_t = wsjrdp2027.merge_mail_addresses(
            m_t["mailing_to"], m_t["mailing_cc"], m_t["mailing_bcc"], default=[]
        )
        all_f = wsjrdp2027.merge_mail_addresses(
            m_f["mailing_to"], m_f["mailing_cc"], m_f["mailing_bcc"], default=[]
        )
        assert set(all_f) < set(all_t)
