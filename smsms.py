DECLARE @dateStart DATETIME;
DECLARE @dateEnd DATETIME;

DECLARE @partNumberPress VARCHAR(50);

DECLARE @press VARCHAR(MAX) = NULL;
DECLARE @tester VARCHAR(MAX) = NULL;

SET @dateStart = '2026-05-01 00:00:00.000';
SET @dateEnd = '2026-05-16 23:59:00.000';

SET @partNumberPress = '2690557D05';

-- Optional Filters
-- SET @press = '603,605';
-- SET @tester = 'Test93';

SELECT
    TR.DateTime,
    TR.RecordIndex,
    TR.TestCage,
    TR.Operator,
    TR.PartNumber,
    TP.description,
    TR.FixtureNumber,
    TR.StepNumber,
    TR.Description
FROM PwK_Tandc.dbo.vWHVTestResults AS TR

LEFT JOIN PwK_Tandc.dbo.tblTestProgramHeader AS TP
    ON TP.TestProgram = TR.PartNumber

LEFT JOIN PwK_Tandc.dbo.tblRS_cycleData AS C
    ON TR.RecordIndex = C.RecordIndex

WHERE
    TR.RecordIndex IS NOT NULL

    AND TR.RecordIndex IN
    (
        SELECT DISTINCT T.recordindex2

        FROM MXQEFLEX.dbo.TRctrackingCodes AS T

        WHERE
            T.recordindex2 IS NOT NULL

            AND T.DateTimeStation2
                BETWEEN @dateStart AND @dateEnd

            AND T.AreaStation1 = 'Press'

            AND T.RecipeStation1 = @partNumberPress

            AND (
                    @press IS NULL
                    OR T.MachineStation1 IN (
                        SELECT value
                        FROM STRING_SPLIT(@press, ',')
                    )
                )
    )

    AND (
            @tester IS NULL
            OR TestCage IN (
                SELECT value
                FROM STRING_SPLIT(@tester, ',')
            )
        )

ORDER BY
    TR.DateTime,
    TR.StepNumber,
    TR.PartNumber,
    TR.TestCage;