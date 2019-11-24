-- GO
-- DROP TABLE IF EXISTS [tb_1];
CREATE TABLE [tb_1] (
	[varcharValue] VARCHAR(128) NOT NULL,
	[intValue]     INT          NOT NULL,
	CONSTRAINT [uq_tb_1] UNIQUE ([varcharValue]),
	CONSTRAINT [uq_tb_2] UNIQUE ([intValue])
)

-- GO
CREATE TRIGGER [tb_1_trigger]
	BEFORE INSERT ON [tb_1]
	FOR EACH ROW
	BEGIN
		SELECT RAISE(ABORT, 'Wrong data. A value of varcharValue should be one of one, two, three, One hundred, Two hundred')
		WHERE NEW.[varcharValue] NOT IN ('one', 'two', 'three', 'One hundred', 'Two hundred');
	END

-- GO
INSERT INTO [tb_1] VALUES ('one', 1)
-- GO
INSERT INTO [tb_1] VALUES ('two', 2)
-- GO
INSERT INTO [tb_1] VALUES ('three', 3)
