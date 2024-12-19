create type vertex_type as ENUM('player','team','game')


create table vertices(
identifier text,
type vertex_type,
properties JSON,
primary KEY(identifier, type)
)

CREATE TYPE edge_type AS ENUM ('plays_against','shares_team','plays_in','plays_on');

--drop table egdes CASCADE

create table edges(
subject_identifier text,
subject_type vertex_type,
object_identifier text,
object_type vertex_type,
edge_type edge_type,
properties JSON,
primary key(subject_identifier,
			subject_type,
			object_identifier,
			object_type,
			edge_type)
)
