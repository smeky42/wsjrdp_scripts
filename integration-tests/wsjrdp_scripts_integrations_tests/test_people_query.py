from __future__ import annotations

import pytest
import wsjrdp2027


class Test_Query_Tags:
    def test__query_people_with_warteliste_like_tag(
        self, ctx: wsjrdp2027.WsjRdpContext
    ):
        where = wsjrdp2027.PeopleWhere(tag={"op": "ilike", "expr": "%Warteliste%"})
        with ctx.psycopg_connect() as conn:
            df = wsjrdp2027.load_people_dataframe(conn, where=where)

        tag_set = set()
        for _, row in df.iterrows():
            tag_list = row['tag_list']
            assert any('warteliste' in tag.lower() for tag in tag_list)
            tag_set.update(tag for tag in tag_list if 'warteliste' in tag.lower())

        tags = sorted(tag_set)
        print(tags)
        assert len(tags) >= 2
