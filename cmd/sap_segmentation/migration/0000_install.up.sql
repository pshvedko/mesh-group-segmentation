create table if not exists segment
(
    id             bigserial primary key,
    address_sap_id varchar(255) not null unique,
    adr_segment    varchar(16),
    segment_id     bigint
);
