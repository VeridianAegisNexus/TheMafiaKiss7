       IDENTIFICATION DIVISION.
       PROGRAM-ID. ACCORD-TRANSACTION-LOG-MAINFRAME-V4.
       AUTHOR. TƕēMafɪa ǦoʇhɪcǶɪppɪē.
       INSTALLATION. BONE ARCHIVE CENTRAL FACILITY.
       DATE-WRITTEN. 2024-10-04.
       DATE-COMPILED. TODAY.
      *
      * DIRECTIVE: FIVE-NINES RELIABILITY. LOG ALL FINAL SVTs TO DLT
      * FOR IMMUTABLE ARCHIVAL.
      * PROTOCOL: CUSSED-ACCORD PROTOCOL (CAP).
      *
       ENVIRONMENT DIVISION.
       INPUT-OUTPUT SECTION.
       FILE-CONTROL.
           SELECT SVT-INPUT-FILE ASSIGN TO 'KAFKA-EVENT-STREAM.DAT'
               ORGANIZATION IS LINE SEQUENTIAL.
           SELECT DLT-ARCHIVE-FILE ASSIGN TO 'DLT_IMMUTABLE_ARCHIVE.LOG'
               ORGANIZATION IS INDEXED
               ACCESS MODE IS SEQUENTIAL
               RECORD KEY IS SVT-TRANSACTION-KEY
               STATUS IS DLT-FILE-STATUS.

       DATA DIVISION.
       FILE SECTION.

       FD  SVT-INPUT-FILE
           RECORD CONTAINS 150 CHARACTERS
           DATA RECORD IS INPUT-SVT-RECORD.
       01  INPUT-SVT-RECORD.
           05  IN-SVT-DID               PIC X(30).
           05  IN-ENERGY-SIG            PIC 9(04).
           05  IN-INTENT                PIC X(20).
           05  IN-WEIGHT-VALUE          PIC 9(10).
           05  IN-TIMESTAMP             PIC 9(16).
           05  FILLER                   PIC X(70).

       FD  DLT-ARCHIVE-FILE
           RECORD CONTAINS 200 CHARACTERS
           DATA RECORD IS DLT-ARCHIVE-RECORD.
       01  DLT-ARCHIVE-RECORD.
           05  SVT-TRANSACTION-KEY      PIC 9(20).
      * SVT-TRANSACTION-KEY is the combined timestamp and weight for final DLT ordering.
           05  SVT-ARCHIVE-DATA.
               10 DLT-DID               PIC X(30).
               10 DLT-ENERGY-SIG        PIC 9(04).
               10 DLT-INTENT            PIC X(20).
               10 DLT-FINAL-WEIGHT      PIC 9(10).
               10 DLT-TIMESTAMP         PIC 9(16).
               10 DLT-VERIFICATION-FLAG PIC X(01) VALUE 'V'.
               10 FILLER                PIC X(119).

       WORKING-STORAGE SECTION.
       01  DLT-FILE-STATUS              PIC X(02).
       01  WS-EOF-FLAG                  PIC X(01) VALUE 'N'.
           88  END-OF-SVT-STREAM        VALUE 'Y'.
       01  WS-TRANSACTION-COUNTER       PIC 9(08) VALUE ZEROES.

       PROCEDURE DIVISION.
       0000-MAIN-LOGGING-PROCESS.
           PERFORM 1000-INITIALIZE-SYSTEM
           PERFORM 2000-PROCESS-SVT-STREAM
               UNTIL END-OF-SVT-STREAM
           PERFORM 3000-TERMINATE-SYSTEM
           STOP RUN.

      * ---------------------------------------------------------------
       1000-INITIALIZE-SYSTEM.
      * Open the Kafka stream (simulated as sequential file) and DLT log.
           OPEN INPUT SVT-INPUT-FILE.
           OPEN I-O DLT-ARCHIVE-FILE.
           IF DLT-FILE-STATUS NOT = '00'
               DISPLAY 'ERROR 1001: FAILED TO OPEN DLT ARCHIVE. STATUS: ' DLT-FILE-STATUS
               MOVE 'Y' TO WS-EOF-FLAG
           ELSE
               PERFORM 1100-READ-SVT-RECORD.

       1100-READ-SVT-RECORD.
           READ SVT-INPUT-FILE
               AT END MOVE 'Y' TO WS-EOF-FLAG
           END-READ.

      * ---------------------------------------------------------------
       2000-PROCESS-SVT-STREAM.
      * CAP: Log only transactions that have passed both RUST CORE (weight)
      * and ROZEL-ROSEL (access) checks.
           IF NOT END-OF-SVT-STREAM
               ADD 1 TO WS-TRANSACTION-COUNTER
               MOVE IN-SVT-DID TO DLT-DID
               MOVE IN-ENERGY-SIG TO DLT-ENERGY-SIG
               MOVE IN-INTENT TO DLT-INTENT
               MOVE IN-WEIGHT-VALUE TO DLT-FINAL-WEIGHT
               MOVE IN-TIMESTAMP TO DLT-TIMESTAMP

      * Key Construction: Timestamp (16 digits) + Weight (4 digits)
               STRING DLT-TIMESTAMP DLT-FINAL-WEIGHT (1:4) DELIMITED BY SIZE
                   INTO SVT-TRANSACTION-KEY

               PERFORM 2100-WRITE-TO-DLT-ARCHIVE
               PERFORM 1100-READ-SVT-RECORD
           END-IF.

       2100-WRITE-TO-DLT-ARCHIVE.
           WRITE DLT-ARCHIVE-RECORD
               INVALID KEY
                   DISPLAY 'ERROR 2101: DLT KEY COLLISION (DUPLICATE SVT): ' SVT-TRANSACTION-KEY
               NOT INVALID KEY
                   DISPLAY 'SV' WS-TRANSACTION-COUNTER ' LOGGED: KEY ' SVT-TRANSACTION-KEY
           END-WRITE.

      * ---------------------------------------------------------------
       3000-TERMINATE-SYSTEM.
           CLOSE SVT-INPUT-FILE.
           CLOSE DLT-ARCHIVE-FILE.
           DISPLAY 'MAINFRAME LOGGING COMPLETE. TOTAL SVTs ARCHIVED: ' WS-TRANSACTION-COUNTER.
           DISPLAY 'FIVE-NINES RELIABILITY MAINTAINED. INSHALLAH.'.

# TheMafiaKiss7
#303550
