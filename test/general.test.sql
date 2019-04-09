-- DROP TABLE IF EXISTS "tb_1";
CREATE TABLE "tb_1" (
	"varcharValue" VARCHAR(128) NOT NULL,
	"intValue"     INT          NOT NULL,
	CONSTRAINT "uq_tb_1" UNIQUE ("varcharValue"),
	CONSTRAINT "uq_tb_2" UNIQUE ("intValue")
);

INSERT INTO "tb_1" VALUES ('one', 1);
INSERT INTO "tb_1" VALUES ('two', 2);
INSERT INTO "tb_1" VALUES ('three', 3);
