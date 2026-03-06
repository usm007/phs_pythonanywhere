from flask import Flask, render_template, request, redirect, url_for, flash, Response, send_from_directory, session, abort
import os
import csv
import io
import re
import sqlite3
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get("PHS_SECRET_KEY", "change-me-in-env")

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    pymysql = None

# TiDB configuration (set via environment variables)
DB_HOST = os.environ.get("TIDB_HOST", "")
DB_USER = os.environ.get("TIDB_USER", "")
DB_PASSWORD = os.environ.get("TIDB_PASSWORD", "")
DB_NAME = os.environ.get("TIDB_DATABASE", "test")
DB_PORT = int(os.environ.get("TIDB_PORT", "4000"))
DB_MODE = os.environ.get("PHS_DB_MODE", "sqlite").lower()
DB_CONNECT_TIMEOUT_SECONDS = int(os.environ.get("PHS_DB_CONNECT_TIMEOUT", "3"))
SEED_SAMPLE_DATA = os.environ.get("PHS_SEED_SAMPLE_DATA", "false").lower() == "true"
BATCH_DELETE_PASSWORD = os.environ.get("PHS_BATCH_DELETE_PASSWORD", "phs")

if DB_MODE not in {"auto", "tidb", "sqlite"}:
    DB_MODE = "auto"

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
LOCAL_DB_FILE = os.path.join(THIS_FOLDER, "school_marks.db")


def _connect_tidb():
    if pymysql is None:
        raise RuntimeError("PyMySQL is not installed")

    if not (DB_HOST and DB_USER and DB_PASSWORD):
        raise RuntimeError("TiDB credentials are not configured")

    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        ssl_verify_cert=True,
        ssl_verify_identity=True,
        connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
        read_timeout=DB_CONNECT_TIMEOUT_SECONDS,
        write_timeout=DB_CONNECT_TIMEOUT_SECONDS,
    )


def _connect_sqlite():
    conn = sqlite3.connect(LOCAL_DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_db_connection():
    if DB_MODE == "sqlite":
        return _connect_sqlite()
    if DB_MODE == "tidb":
        return _connect_tidb()

    # auto mode: prefer cloud, gracefully fall back offline.
    try:
        return _connect_tidb()
    except Exception:
        return _connect_sqlite()


def _is_sqlite_connection(conn):
    return isinstance(conn, sqlite3.Connection)


def _adapt_query_for_backend(conn, query):
    if not _is_sqlite_connection(conn):
        return query
    return query.replace("%s", "?").replace("AS UNSIGNED", "AS INTEGER")


def fetch_all(conn, query, params=()):
    cursor = conn.cursor()
    try:
        cursor.execute(_adapt_query_for_backend(conn, query), params)
        return cursor.fetchall()
    finally:
        cursor.close()


def fetch_one(conn, query, params=()):
    cursor = conn.cursor()
    try:
        cursor.execute(_adapt_query_for_backend(conn, query), params)
        return cursor.fetchone()
    finally:
        cursor.close()


def execute_stmt(conn, query, params=()):
    cursor = conn.cursor()
    try:
        cursor.execute(_adapt_query_for_backend(conn, query), params)
    finally:
        cursor.close()


def executemany_stmt(conn, query, rows):
    cursor = conn.cursor()
    try:
        cursor.executemany(_adapt_query_for_backend(conn, query), rows)
    finally:
        cursor.close()


def log_change(
    conn,
    action,
    entity_type,
    details,
    class_name=None,
    subject=None,
    affected_count=1,
):
    execute_stmt(
        conn,
        """
        INSERT INTO change_logs (
            action, entity_type, class_name, subject, details, affected_count, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            action,
            entity_type,
            class_name,
            subject,
            details,
            affected_count,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )


def get_recent_logs(limit=20):
    conn = get_db_connection()
    try:
        rows = fetch_all(
            conn,
            """
            SELECT id, action, entity_type, class_name, subject, details, affected_count, created_at
            FROM change_logs
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return rows
    finally:
        conn.close()

SUBJECTS_DICT = {
    "Class 6": [
        "Assamese",
        "English",
        "General Science",
        "General Mathematics",
        "Social Science",
        "Hindi",
    ],
    "Class 7": [
        "Assamese",
        "English",
        "General Science",
        "General Mathematics",
        "Social Science",
        "Hindi",
    ],
    "Class 8(A)": [
        "Assamese",
        "English",
        "General Science",
        "General Mathematics",
        "Social Science",
        "Hindi",
    ],
    "Class 8(B)": [
        "Assamese",
        "English",
        "General Science",
        "General Mathematics",
        "Social Science",
        "Hindi",
    ],
    "Class 9(A)": [
        "Assamese",
        "English",
        "General Science",
        "General Mathematics",
        "Social Science",
        "Elective: Hindi",
        "Elective: History",
        "Elective: Agriculture & Horticulture NSQF",
        "Elective: Healthcare NSQF",
    ],
    "Class 9(B)": [
        "Assamese",
        "English",
        "General Science",
        "General Mathematics",
        "Social Science",
        "Elective: Hindi",
        "Elective: History",
        "Elective: Agriculture & Horticulture NSQF",
        "Elective: Healthcare NSQF",
    ],
}

CLASS_DISPLAY_MAP = {
    "Class 6": "Class VI",
    "Class 7": "Class VII",
    "Class 8(A)": "Class VIII(A)",
    "Class 8(B)": "Class VIII(B)",
    "Class 9(A)": "Class IX(A)",
    "Class 9(B)": "Class IX(B)",
}


def class_label(class_name):
    return CLASS_DISPLAY_MAP.get(class_name, class_name)


def _normalize_class_token(value):
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


CLASS_IMPORT_ALIASES = {
    # Canonical names
    _normalize_class_token("Class 6"): "Class 6",
    _normalize_class_token("Class 7"): "Class 7",
    _normalize_class_token("Class 8(A)"): "Class 8(A)",
    _normalize_class_token("Class 8(B)"): "Class 8(B)",
    _normalize_class_token("Class 9(A)"): "Class 9(A)",
    _normalize_class_token("Class 9(B)"): "Class 9(B)",
    # Shorthand styles accepted in CSV
    "6": "Class 6",
    "6a": "Class 6",
    "6b": "Class 6",
    "class6": "Class 6",
    "7": "Class 7",
    "7a": "Class 7",
    "7b": "Class 7",
    "class7": "Class 7",
    "8a": "Class 8(A)",
    "class8a": "Class 8(A)",
    "8b": "Class 8(B)",
    "class8b": "Class 8(B)",
    "9a": "Class 9(A)",
    "class9a": "Class 9(A)",
    "9b": "Class 9(B)",
    "class9b": "Class 9(B)",
}


def canonicalize_class_name(value):
    return CLASS_IMPORT_ALIASES.get(_normalize_class_token(value))


def get_csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = os.urandom(24).hex()
        session["_csrf_token"] = token
    return token




@app.before_request
def validate_post_requests():
    if request.method != "POST":
        return

    csrf_token = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
    if not csrf_token or csrf_token != session.get("_csrf_token"):
        abort(400, description="Invalid CSRF token")


@app.context_processor
def inject_class_label_helper():
    return {
        "class_label": class_label,
        "csrf_token": get_csrf_token,
    }


def init_db():
    conn = get_db_connection()
    try:
        if _is_sqlite_connection(conn):
            execute_stmt(
                conn,
                """
                CREATE TABLE IF NOT EXISTS students (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    roll_no TEXT NOT NULL,
                    name TEXT NOT NULL,
                    class_name TEXT NOT NULL
                )
                """,
            )
            execute_stmt(
                conn,
                """
                CREATE TABLE IF NOT EXISTS marks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    student_id INTEGER NOT NULL,
                    subject TEXT NOT NULL,
                    marks_obtained INTEGER NOT NULL,
                    UNIQUE(student_id, subject),
                    FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
                )
                """,
            )
            execute_stmt(
                conn,
                """
                CREATE TABLE IF NOT EXISTS change_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    class_name TEXT,
                    subject TEXT,
                    details TEXT NOT NULL,
                    affected_count INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                )
                """,
            )
        else:
            execute_stmt(
                conn,
                """
                CREATE TABLE IF NOT EXISTS students (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    roll_no VARCHAR(32) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    class_name VARCHAR(32) NOT NULL
                )
                """,
            )
            execute_stmt(
                conn,
                """
                CREATE TABLE IF NOT EXISTS marks (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    student_id BIGINT NOT NULL,
                    subject VARCHAR(255) NOT NULL,
                    marks_obtained INT NOT NULL,
                    UNIQUE KEY uq_student_subject (student_id, subject),
                    CONSTRAINT fk_marks_student
                        FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE
                )
                """,
            )
            execute_stmt(
                conn,
                """
                CREATE TABLE IF NOT EXISTS change_logs (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    action VARCHAR(64) NOT NULL,
                    entity_type VARCHAR(64) NOT NULL,
                    class_name VARCHAR(64) NULL,
                    subject VARCHAR(255) NULL,
                    details VARCHAR(1000) NOT NULL,
                    affected_count INT NOT NULL DEFAULT 1,
                    created_at VARCHAR(32) NOT NULL
                )
                """,
            )

        count_row = fetch_one(conn, "SELECT COUNT(*) AS c FROM students")
        count = count_row["c"] if count_row else 0
        if count == 0 and SEED_SAMPLE_DATA:
            sample_rows = []
            for class_name in SUBJECTS_DICT.keys():
                for i in range(1, 6):
                    sample_rows.append((str(i), f"Student {i} {class_name}", class_name))
            executemany_stmt(
                conn,
                "INSERT INTO students (roll_no, name, class_name) VALUES (%s, %s, %s)",
                sample_rows,
            )
        conn.commit()
    finally:
        conn.close()


def get_class_results(class_name):
    conn = get_db_connection()
    try:
        students = fetch_all(
            conn,
            "SELECT id, roll_no, name FROM students WHERE class_name = %s ORDER BY CAST(roll_no AS UNSIGNED), roll_no",
            (class_name,),
        )
        marks = fetch_all(
            conn,
            """
            SELECT m.student_id, m.subject, m.marks_obtained
            FROM marks m
            JOIN students s ON m.student_id = s.id
            WHERE s.class_name = %s
            """,
            (class_name,),
        )
    finally:
        conn.close()

    is_class_9 = "Class 9" in class_name
    max_per_sub = 100 if is_class_9 else 50
    subjects = SUBJECTS_DICT[class_name]
    grand_total_possible = len(subjects) * max_per_sub

    m_dict = {}
    for mark_row in marks:
        if mark_row["student_id"] not in m_dict:
            m_dict[mark_row["student_id"]] = {}
        m_dict[mark_row["student_id"]][mark_row["subject"]] = mark_row["marks_obtained"]

    results = []
    for student in students:
        student_marks = m_dict.get(student["id"], {})
        total = sum(student_marks.values())
        percentage = round((total / grand_total_possible) * 100, 2) if total > 0 else 0
        status = "PASS" if percentage >= 30 else "FAIL"
        if not student_marks:
            status = "ABSENT"

        results.append(
            {
                "id": student["id"],
                "roll_no": student["roll_no"],
                "name": student["name"],
                "marks": student_marks,
                "total": total,
                "total_possible": grand_total_possible,
                "max_per_subject": max_per_sub,
                "percentage": percentage,
                "status": status,
            }
        )

    # Assign ranks: only PASS students ranked by total desc, ties get same rank
    appeared = [r for r in results if r["status"] != "ABSENT"]
    appeared_sorted = sorted(appeared, key=lambda x: x["total"], reverse=True)
    rank = 1
    for i, r in enumerate(appeared_sorted):
        if i > 0 and r["total"] < appeared_sorted[i - 1]["total"]:
            rank = i + 1
        r["rank"] = rank if r["status"] == "PASS" else "—"
    for r in results:
        if "rank" not in r:
            r["rank"] = "—"

    return results, subjects


def calculate_class_stats(results):
    total_students = len(results)
    pass_count = sum(1 for row in results if row["status"] == "PASS")
    fail_count = sum(1 for row in results if row["status"] == "FAIL")
    absent_count = sum(1 for row in results if row["status"] == "ABSENT")
    appeared_count = total_students - absent_count
    pass_rate = round((pass_count / appeared_count) * 100, 2) if appeared_count else 0

    return {
        "total_students": total_students,
        "appeared_count": appeared_count,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "absent_count": absent_count,
        "pass_rate": pass_rate,
    }


@app.route("/")
def index():
    return render_template("index.html", subjects_dict=SUBJECTS_DICT)


@app.route("/logs")
def view_logs():
    recent_logs = get_recent_logs(300)
    return render_template("logs.html", recent_logs=recent_logs)


@app.route("/results_center")
def results_center():
    class_cards = []
    for class_name in SUBJECTS_DICT.keys():
        results, _ = get_class_results(class_name)
        class_cards.append(
            {
                "class_name": class_name,
                "stats": calculate_class_stats(results),
            }
        )
    return render_template("results_center.html", class_cards=class_cards)


@app.route("/subject_entry", methods=["GET", "POST"])
def subject_entry():
    if request.method == "POST":
        class_name = request.form["class_name"]
        subject = request.form["subject"]

        max_per_sub = 100 if "Class 9" in class_name else 50
        inserted_count = 0
        updated_count = 0

        conn = get_db_connection()
        try:
            for key, value in request.form.items():
                if key.startswith("mark_") and value.strip() != "":
                    student_id = key.split("_")[1]
                    try:
                        mark = int(value)
                    except ValueError:
                        flash("Marks must be whole numbers only.", "danger")
                        return redirect(
                            url_for("subject_entry", class_name=class_name, subject=subject)
                        )

                    if mark < 0 or mark > max_per_sub:
                        flash(
                            f"Invalid marks for {subject}. Enter between 0 and {max_per_sub}.",
                            "danger",
                        )
                        return redirect(
                            url_for(
                                "subject_entry", class_name=class_name, subject=subject
                            )
                        )

                    existing = fetch_one(
                        conn,
                        "SELECT id FROM marks WHERE student_id = %s AND subject = %s",
                        (student_id, subject),
                    )
                    if existing:
                        execute_stmt(
                            conn,
                            "UPDATE marks SET marks_obtained = %s WHERE student_id = %s AND subject = %s",
                            (mark, student_id, subject),
                        )
                        updated_count += 1
                    else:
                        execute_stmt(
                            conn,
                            "INSERT INTO marks (student_id, subject, marks_obtained) VALUES (%s, %s, %s)",
                            (student_id, subject, mark),
                        )
                        inserted_count += 1
            total_changed = inserted_count + updated_count
            if total_changed:
                log_change(
                    conn,
                    action="save_marks",
                    entity_type="marks",
                    class_name=class_name,
                    subject=subject,
                    details=f"Saved marks for {class_name} ({subject}): {inserted_count} inserted, {updated_count} updated.",
                    affected_count=total_changed,
                )
            conn.commit()
        finally:
            conn.close()
        flash(f"✅ Marks saved for {class_name} ({subject})", "success")
        return redirect(url_for("index"))

    class_name = request.args.get("class_name")
    subject = request.args.get("subject")
    if not class_name or not subject:
        flash("Select class and subject first.", "warning")
        return redirect(url_for("index"))
    if class_name not in SUBJECTS_DICT:
        flash("Invalid class selected.", "danger")
        return redirect(url_for("index"))
    if subject not in SUBJECTS_DICT[class_name]:
        flash("Invalid subject selected.", "danger")
        return redirect(url_for("index"))

    conn = get_db_connection()
    try:
        students = fetch_all(
            conn,
            """
            SELECT s.id, s.roll_no, s.name, m.marks_obtained
            FROM students s
            LEFT JOIN marks m ON s.id = m.student_id AND m.subject = %s
            WHERE s.class_name = %s
            ORDER BY CAST(s.roll_no AS UNSIGNED), s.roll_no
            """,
            (subject, class_name),
        )
    finally:
        conn.close()

    max_per_subject = 100 if "Class 9" in class_name else 50
    return render_template(
        "subject_entry.html",
        class_name=class_name,
        subject=subject,
        students=students,
        max_per_subject=max_per_subject,
    )


@app.route("/grid_entry", methods=["GET", "POST"])
def grid_entry():
    class_name = (
        request.args.get("class_name")
        if request.method == "GET"
        else request.form["class_name"]
    )

    if not class_name or class_name not in SUBJECTS_DICT:
        flash("Select a valid class.", "warning")
        return redirect(url_for("index"))

    subjects = SUBJECTS_DICT[class_name]
    max_per_subject = 100 if "Class 9" in class_name else 50

    conn = get_db_connection()
    if request.method == "POST":
        inserted_count = 0
        updated_count = 0
        try:
            for key, value in request.form.items():
                if key.startswith("mark_") and value.strip() != "":
                    parts = key.split("_", 2)
                    student_id, subject = parts[1], parts[2]
                    try:
                        mark = int(value)
                    except ValueError:
                        flash("Marks must be whole numbers only.", "danger")
                        return redirect(url_for("grid_entry", class_name=class_name))

                    if subject not in subjects:
                        flash("Invalid subject in grid payload.", "danger")
                        return redirect(url_for("grid_entry", class_name=class_name))

                    if mark < 0 or mark > max_per_subject:
                        flash(
                            f"Invalid marks in grid. Enter between 0 and {max_per_subject}.",
                            "danger",
                        )
                        return redirect(url_for("grid_entry", class_name=class_name))

                    existing = fetch_one(
                        conn,
                        "SELECT id FROM marks WHERE student_id = %s AND subject = %s",
                        (student_id, subject),
                    )
                    if existing:
                        execute_stmt(
                            conn,
                            "UPDATE marks SET marks_obtained = %s WHERE student_id = %s AND subject = %s",
                            (mark, student_id, subject),
                        )
                        updated_count += 1
                    else:
                        execute_stmt(
                            conn,
                            "INSERT INTO marks (student_id, subject, marks_obtained) VALUES (%s, %s, %s)",
                            (student_id, subject, mark),
                        )
                        inserted_count += 1
            total_changed = inserted_count + updated_count
            if total_changed:
                log_change(
                    conn,
                    action="save_grid_marks",
                    entity_type="marks",
                    class_name=class_name,
                    details=f"Saved grid marks for {class_name}: {inserted_count} inserted, {updated_count} updated.",
                    affected_count=total_changed,
                )
            conn.commit()
        finally:
            conn.close()
        flash(f"✅ Master Grid saved for {class_name}!", "success")
        return redirect(url_for("index"))

    students = fetch_all(
        conn,
        "SELECT id, roll_no, name FROM students WHERE class_name = %s ORDER BY CAST(roll_no AS UNSIGNED), roll_no",
        (class_name,),
    )

    marks_dict = {}
    rows = fetch_all(
        conn,
        """
        SELECT m.student_id, m.subject, m.marks_obtained
        FROM marks m
        JOIN students s ON s.id = m.student_id
        WHERE s.class_name = %s
        """,
        (class_name,),
    )
    for row in rows:
        if row["student_id"] not in marks_dict:
            marks_dict[row["student_id"]] = {}
        marks_dict[row["student_id"]][row["subject"]] = row["marks_obtained"]

    conn.close()
    return render_template(
        "grid_entry.html",
        class_name=class_name,
        subjects=subjects,
        students=students,
        marks_dict=marks_dict,
        max_per_subject=max_per_subject,
    )


@app.route("/view")
def view_marks():
    grouped = {}
    classes = list(SUBJECTS_DICT.keys())

    for class_name in classes:
        results, subjects = get_class_results(class_name)
        stats = calculate_class_stats(results)
        marks_entered = sum(len(row["marks"]) for row in results)
        marks_expected = len(results) * len(subjects)
        students_with_marks = sum(1 for row in results if len(row["marks"]) > 0)

        grouped[class_name] = {
            "results": results,
            "subjects": subjects,
            "stats": stats,
            "marks_entered": marks_entered,
            "marks_expected": marks_expected,
            "students_with_marks": students_with_marks,
        }

    selected_class = request.args.get("class_name")
    if selected_class not in grouped:
        selected_class = classes[0] if classes else None

    selected_data = grouped.get(
        selected_class,
        {
            "results": [],
            "subjects": [],
            "stats": {
                "total_students": 0,
                "appeared_count": 0,
                "pass_count": 0,
                "fail_count": 0,
                "absent_count": 0,
                "pass_rate": 0,
            },
            "marks_entered": 0,
            "marks_expected": 0,
            "students_with_marks": 0,
        },
    )

    return render_template(
        "view.html",
        grouped_data=grouped,
        classes=classes,
        selected_class=selected_class,
        selected_data=selected_data,
    )


@app.route("/download_csv/<string:class_name>")
def download_csv(class_name):
    if class_name not in SUBJECTS_DICT:
        flash("Invalid class.", "danger")
        return redirect(url_for("results_center"))

    results, subjects = get_class_results(class_name)
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(["Roll", "Name"] + subjects + ["Total", "%", "Result"])
    for row in results:
        writer.writerow(
            [row["roll_no"], row["name"]]
            + [row["marks"].get(subject, "") for subject in subjects]
            + [row["total"], row["percentage"], row["status"]]
        )

    return Response(
        stream.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={class_name}.csv"},
    )


@app.route("/download_student_import_sample/single/<string:class_name>")
def download_student_import_sample_single(class_name):
    if class_name not in SUBJECTS_DICT:
        flash("Invalid class for sample download.", "danger")
        return redirect(url_for("manage_students"))

    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(["roll_no", "name"])
    writer.writerow(["1", "Aman Das"])
    writer.writerow(["2", "Riya Sharma"])
    writer.writerow(["3", "Neel Bora"])

    return Response(
        stream.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=student_import_single_{class_name}.csv"
        },
    )


@app.route("/download_student_import_sample/multi")
def download_student_import_sample_multi():
    classes = list(SUBJECTS_DICT.keys())
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(["class_name", "roll_no", "name"])

    if classes:
        writer.writerow([classes[0], "1", "Aman Das"])
    if len(classes) > 1:
        writer.writerow([classes[1], "12", "Riya Sharma"])
    if len(classes) > 2:
        writer.writerow([classes[2], "4", "Neel Bora"])

    return Response(
        stream.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=student_import_multi.csv"},
    )


@app.route("/print_class_ledger/<string:class_name>")
def print_class_ledger(class_name):
    if class_name not in SUBJECTS_DICT:
        flash("Invalid class.", "danger")
        return redirect(url_for("results_center"))

    results, subjects = get_class_results(class_name)
    stats = calculate_class_stats(results)
    return render_template(
        "print_ledger.html",
        class_name=class_name,
        results=results,
        subjects=subjects,
        stats=stats,
    )


@app.route("/report_cards/<string:class_name>")
def report_cards_bulk(class_name):
    if class_name not in SUBJECTS_DICT:
        flash("Invalid class.", "danger")
        return redirect(url_for("results_center"))

    results, subjects = get_class_results(class_name)
    return render_template(
        "report_card.html", class_name=class_name, results=results, subjects=subjects
    )


@app.route("/report_cards/<string:class_name>/individual")
def report_cards_individual_list(class_name):
    if class_name not in SUBJECTS_DICT:
        flash("Invalid class.", "danger")
        return redirect(url_for("results_center"))

    results, _ = get_class_results(class_name)
    return render_template(
        "individual_report_cards.html", class_name=class_name, results=results
    )


@app.route("/report_cards/<string:class_name>/individual/<int:student_id>")
def report_card_individual(class_name, student_id):
    if class_name not in SUBJECTS_DICT:
        flash("Invalid class.", "danger")
        return redirect(url_for("results_center"))

    results, subjects = get_class_results(class_name)
    selected_student = next((row for row in results if row["id"] == student_id), None)
    if not selected_student:
        flash("Student not found in selected class.", "warning")
        return redirect(url_for("report_cards_individual_list", class_name=class_name))

    return render_template(
        "report_card.html",
        class_name=class_name,
        results=[selected_student],
        subjects=subjects,
        single_mode=True,
    )


@app.route("/manage_students", methods=["GET", "POST"])
def manage_students():
    conn = get_db_connection()
    if request.method == "POST":
        class_name = request.form.get("class_name")
        added_count = 0
        updated_count = 0

        new_roll = request.form.get("new_roll", "").strip()
        new_name = request.form.get("new_name", "").strip()
        if new_roll or new_name:
            if not class_name:
                conn.close()
                flash("Select class before adding a student.", "warning")
                return redirect(url_for("manage_students"))
            if not new_roll or not new_name:
                conn.close()
                flash("Enter both roll and name for new student.", "warning")
                return redirect(url_for("manage_students", class_name=class_name))

            duplicate = fetch_one(
                conn,
                "SELECT id FROM students WHERE class_name = %s AND roll_no = %s",
                (class_name, new_roll),
            )
            if duplicate:
                conn.close()
                flash(f"Roll {new_roll} already exists in {class_name}.", "danger")
                return redirect(url_for("manage_students", class_name=class_name))

            execute_stmt(
                conn,
                "INSERT INTO students (roll_no, name, class_name) VALUES (%s, %s, %s)",
                (new_roll, new_name, class_name),
            )
            added_count += 1

        for key, value in request.form.items():
            if key.startswith("name_"):
                student_id = key.split("_")[1]
                roll_no = request.form.get(f"roll_{student_id}", "").strip()
                name = value.strip()
                if not name or not roll_no:
                    continue

                existing = fetch_one(
                    conn,
                    "SELECT id FROM students WHERE class_name = %s AND roll_no = %s AND id != %s",
                    (class_name, roll_no, student_id),
                )
                if existing:
                    conn.close()
                    flash(f"Roll {roll_no} is duplicated. Use unique rolls in class.", "danger")
                    return redirect(url_for("manage_students", class_name=class_name))

                current_row = fetch_one(
                    conn,
                    "SELECT roll_no, name FROM students WHERE id = %s",
                    (student_id,),
                )
                if not current_row:
                    continue
                if current_row["roll_no"] == roll_no and current_row["name"] == name:
                    continue

                execute_stmt(
                    conn,
                    "UPDATE students SET name = %s, roll_no = %s WHERE id = %s",
                    (name, roll_no, student_id),
                )
                updated_count += 1

        total_changed = added_count + updated_count
        if total_changed:
            log_change(
                conn,
                action="manage_students_save",
                entity_type="students",
                class_name=class_name,
                details=f"Saved student changes for {class_name}: {added_count} added, {updated_count} updated.",
                affected_count=total_changed,
            )

        conn.commit()
        conn.close()
        flash("✅ Updated!", "success")
        return redirect(url_for("manage_students", class_name=class_name))

    class_name = request.args.get("class_name")
    if class_name:
        students = fetch_all(
            conn,
            "SELECT * FROM students WHERE class_name = %s ORDER BY CAST(roll_no AS UNSIGNED), roll_no",
            (class_name,),
        )
    else:
        students = []
    conn.close()
    return render_template(
        "manage_students.html",
        class_name=class_name,
        students=students,
        classes=list(SUBJECTS_DICT.keys()),
    )


@app.route("/delete_student/<int:student_id>", methods=["POST"])
def delete_student(student_id):
    class_name = request.form.get("class_name") or request.args.get("class_name")
    conn = get_db_connection()
    student = fetch_one(
        conn,
        "SELECT id, roll_no, name, class_name FROM students WHERE id = %s",
        (student_id,),
    )
    if not student:
        conn.close()
        flash("Student not found.", "warning")
        return redirect(url_for("manage_students", class_name=class_name))

    execute_stmt(conn, "DELETE FROM students WHERE id = %s", (student_id,))
    log_change(
        conn,
        action="delete_student",
        entity_type="students",
        class_name=student["class_name"],
        details=f"Deleted student {student['name']} (roll {student['roll_no']}) from {student['class_name']}.",
        affected_count=1,
    )
    conn.commit()
    conn.close()
    flash("Student deleted successfully.", "success")
    return redirect(url_for("manage_students", class_name=class_name))


def _parse_student_csv(file_stream, single_class_name=None):
    """Parse student CSV and return rows plus parse stats.

    Supports two formats:
      A) Single-class (single_class_name provided): columns roll_no,name
      B) Multi-class  (single_class_name is None):  columns class_name,roll_no,name

    Accepted header aliases (case-insensitive):
      roll_no: roll_no, roll, rollno, roll number
      name:    name, student_name, student name
      class:   class_name, class, class name

    Returns (rows, stats):
      rows: list[(class_name, roll_no, name)]
      stats: {
        total_rows, skipped_empty, duplicate_rows_in_file
      }
    """

    def _normalize_header(value):
        value = (value or "").strip().lower()
        value = re.sub(r"[^a-z0-9]+", "_", value)
        return value.strip("_")

    def _find_header(fieldnames, aliases):
        for raw in fieldnames:
            if _normalize_header(raw) in aliases:
                return raw
        return None

    # PythonAnywhere/Werkzeug may provide upload streams that don't implement
    # the full file API expected by TextIOWrapper (e.g. readable()).
    raw = file_stream.read()
    if isinstance(raw, bytes):
        decoded = raw.decode("utf-8-sig", errors="replace")
    else:
        decoded = str(raw)

    reader = csv.DictReader(io.StringIO(decoded))
    fieldnames = reader.fieldnames or []

    if not fieldnames:
        raise ValueError("CSV has no header row.")

    roll_key = _find_header(fieldnames, {"roll_no", "roll", "rollno", "roll_number"})
    name_key = _find_header(fieldnames, {"name", "student_name"})
    class_key = _find_header(fieldnames, {"class_name", "class"})

    required = ["roll_no", "name"] if single_class_name is not None else ["class_name", "roll_no", "name"]
    missing = []
    if not roll_key:
        missing.append("roll_no")
    if not name_key:
        missing.append("name")
    if single_class_name is None and not class_key:
        missing.append("class_name")
    if missing:
        raise ValueError(f"Missing required header(s): {', '.join(missing)}")

    deduped = {}
    skipped_empty = 0
    duplicate_rows_in_file = 0
    total_rows = 0

    for raw_row in reader:
        total_rows += 1

        row = {k: (v.strip() if isinstance(v, str) else (v or "")) for k, v in raw_row.items()}

        if single_class_name is not None:
            roll_no = row.get(roll_key, "")
            name = row.get(name_key, "")
            class_name = single_class_name
        else:
            class_name = row.get(class_key, "")
            roll_no = row.get(roll_key, "")
            name = row.get(name_key, "")

        if not roll_no or not name or not class_name:
            skipped_empty += 1
            continue

        key = (class_name, roll_no)
        if key in deduped:
            duplicate_rows_in_file += 1
        deduped[key] = name

    rows = [(cls, roll, name) for (cls, roll), name in deduped.items()]
    stats = {
        "total_rows": total_rows,
        "skipped_empty": skipped_empty,
        "duplicate_rows_in_file": duplicate_rows_in_file,
    }

    return rows, stats


@app.route("/import_students", methods=["POST"])
def import_students():
    class_name = request.form.get("import_class_name", "").strip() or None
    file = request.files.get("csv_file")
    redirect_class = class_name or request.form.get("current_class_name", "")

    if not file or file.filename == "":
        flash("No CSV file selected.", "warning")
        return redirect(url_for("manage_students", class_name=redirect_class or None))

    filename = (file.filename or "").lower()
    if not filename.endswith(".csv"):
        flash("Please upload a .csv file exported from Excel.", "warning")
        return redirect(url_for("manage_students", class_name=redirect_class or None))

    try:
        rows, parse_stats = _parse_student_csv(file.stream, single_class_name=class_name)
    except Exception as exc:
        flash(f"Failed to read CSV: {exc}", "danger")
        return redirect(url_for("manage_students", class_name=redirect_class or None))

    if not rows:
        skipped_empty = parse_stats.get("skipped_empty", 0)
        duplicates_in_file = parse_stats.get("duplicate_rows_in_file", 0)
        flash(
            (
                "No valid rows found in CSV. "
                f"Skipped empty rows: {skipped_empty}; duplicate rows in file: {duplicates_in_file}."
            ),
            "warning",
        )
        return redirect(url_for("manage_students", class_name=redirect_class or None))

    # Validate and normalize class names against accepted aliases.
    filtered_rows = []
    skipped_invalid_class = 0
    for cls, roll, name in rows:
        canonical_cls = canonicalize_class_name(cls)
        if not canonical_cls:
            skipped_invalid_class += 1
        else:
            filtered_rows.append((canonical_cls, roll, name))
    rows = filtered_rows

    if not rows:
        flash(
            "No importable rows after class validation. "
            f"Invalid class rows: {skipped_invalid_class}.",
            "warning",
        )
        return redirect(url_for("manage_students", class_name=redirect_class or None))

    inserted = 0
    updated = 0
    unchanged = 0
    conn = get_db_connection()
    try:
        for cls, roll, name in rows:
            existing = fetch_one(
                conn,
                "SELECT id, name FROM students WHERE class_name = %s AND roll_no = %s",
                (cls, roll),
            )
            if existing:
                if existing["name"] != name:
                    execute_stmt(
                        conn,
                        "UPDATE students SET name = %s WHERE id = %s",
                        (name, existing["id"]),
                    )
                    updated += 1
                else:
                    unchanged += 1
            else:
                execute_stmt(
                    conn,
                    "INSERT INTO students (roll_no, name, class_name) VALUES (%s, %s, %s)",
                    (roll, name, cls),
                )
                inserted += 1
        total_changed = inserted + updated
        scope = class_name or "multiple classes"
        log_change(
            conn,
            action="import_students_csv",
            entity_type="students",
            class_name=class_name,
            details=(
                f"CSV import for {scope}: {inserted} inserted, {updated} updated, "
                f"{unchanged} unchanged, {parse_stats.get('duplicate_rows_in_file', 0)} duplicate rows merged, "
                f"{parse_stats.get('skipped_empty', 0)} skipped empty, {skipped_invalid_class} invalid class."
            ),
            affected_count=total_changed,
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        flash(f"Import failed: {exc}", "danger")
        return redirect(url_for("manage_students", class_name=redirect_class or None))
    finally:
        conn.close()

    parts = []
    if inserted:
        parts.append(f"{inserted} imported")
    if updated:
        parts.append(f"{updated} updated")
    if unchanged:
        parts.append(f"{unchanged} unchanged")
    skipped_empty = parse_stats.get("skipped_empty", 0)
    duplicates_in_file = parse_stats.get("duplicate_rows_in_file", 0)
    if duplicates_in_file:
        parts.append(f"{duplicates_in_file} duplicate row(s) merged")
    if skipped_empty:
        parts.append(f"{skipped_empty} empty row(s) skipped")
    if skipped_invalid_class:
        parts.append(f"{skipped_invalid_class} invalid class row(s) skipped")
    flash(f"✅ CSV import complete: {', '.join(parts)}.", "success")
    return redirect(url_for("manage_students", class_name=redirect_class or None))


@app.route("/batch_delete_students", methods=["POST"])
def batch_delete_students():
    class_name = request.form.get("class_name", "")
    password = request.form.get("batch_delete_password", "")

    if password != BATCH_DELETE_PASSWORD:
        flash("Incorrect password. Batch delete cancelled.", "danger")
        return redirect(url_for("manage_students", class_name=class_name or None))

    student_ids = request.form.getlist("delete_ids")
    if not student_ids:
        flash("No students selected for deletion.", "warning")
        return redirect(url_for("manage_students", class_name=class_name or None))

    conn = get_db_connection()
    try:
        deleted = 0
        for sid in student_ids:
            try:
                sid_int = int(sid)
            except ValueError:
                continue
            existing = fetch_one(
                conn, "SELECT id FROM students WHERE id = %s", (sid_int,)
            )
            if existing:
                execute_stmt(conn, "DELETE FROM students WHERE id = %s", (sid_int,))
                deleted += 1
        if deleted:
            target = class_name or "multiple classes"
            log_change(
                conn,
                action="batch_delete_students",
                entity_type="students",
                class_name=class_name or None,
                details=f"Batch deleted {deleted} students from {target}.",
                affected_count=deleted,
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        flash(f"Batch delete failed: {exc}", "danger")
        return redirect(url_for("manage_students", class_name=class_name or None))
    finally:
        conn.close()

    flash(f"✅ Deleted {deleted} student(s) and their marks.", "success")
    return redirect(url_for("manage_students", class_name=class_name or None))


@app.route("/clear_student_marks/<int:student_id>", methods=["POST"])
def clear_student_marks(student_id):
    conn = get_db_connection()
    mark_count_row = fetch_one(
        conn,
        "SELECT COUNT(*) AS c FROM marks WHERE student_id = %s",
        (student_id,),
    )
    mark_count = mark_count_row["c"] if mark_count_row else 0
    student = fetch_one(
        conn,
        "SELECT roll_no, name, class_name FROM students WHERE id = %s",
        (student_id,),
    )
    execute_stmt(conn, "DELETE FROM marks WHERE student_id = %s", (student_id,))
    if mark_count:
        if student:
            details = (
                f"Cleared {mark_count} marks entries for {student['name']} "
                f"(roll {student['roll_no']}, {student['class_name']})."
            )
            class_name = student["class_name"]
        else:
            details = f"Cleared {mark_count} marks entries for student_id={student_id}."
            class_name = None
        log_change(
            conn,
            action="clear_student_marks",
            entity_type="marks",
            class_name=class_name,
            details=details,
            affected_count=mark_count,
        )
    conn.commit()
    conn.close()
    flash("Marks cleared.", "success")
    return redirect(url_for("view_marks"))


@app.route("/signature_image")
def signature_image():
    return send_from_directory(THIS_FOLDER, "sign.jpg")


# Ensure DB tables exist when app is imported by WSGI servers (e.g. PythonAnywhere).
init_db()


if __name__ == "__main__":
    app.run(debug=True)
