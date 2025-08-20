# dags/diagnose_git_tls_env.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, ssl, shutil, socket, tempfile, subprocess, logging, stat

def diagnose_git_tls_env(**_):
    log = logging.getLogger("diagnose")
    env = os.environ.copy()

    # 1) Basic environment facts
    log.info("== Basic Env ==")
    log.info("USER=%s", env.get("USER"))
    log.info("HOME=%s", os.path.expanduser("~"))
    log.info("PWD =%s", os.getcwd())
    log.info("PATH=%s", env.get("PATH"))

    # 2) Proxy environment
    log.info("== Proxy Vars ==")
    for k in ("HTTP_PROXY","HTTPS_PROXY","http_proxy","https_proxy","NO_PROXY","no_proxy"):
        if env.get(k):
            log.info("%s=%s", k, env[k])
        else:
            log.info("%s is not set", k)

    # 3) SSL default verify paths (where OpenSSL would look)
    log.info("== OpenSSL verify paths ==")
    vp = ssl.get_default_verify_paths()
    log.info("cafile=%s capath=%s", vp.cafile, vp.capath)
    try:
        import certifi, pathlib
        certifi_path = certifi.where()
        log.info("certifi.where()=%s (exists=%s, size=%s)",
                 certifi_path,
                 os.path.exists(certifi_path),
                 os.path.getsize(certifi_path) if os.path.exists(certifi_path) else "n/a")
    except Exception as e:
        log.warning("certifi not importable: %r", e)
        certifi_path = None

    # 4) Check git binary
    log.info("== Git binary ==")
    git_path = shutil.which("git")
    if git_path:
        log.info("git found at: %s", git_path)
        try:
            out = subprocess.check_output([git_path, "--version"], stderr=subprocess.STDOUT, text=True)
            log.info("git --version: %s", out.strip())
        except Exception as e:
            log.warning("git --version failed: %r", e)
    else:
        log.warning("git NOT found on PATH")

    # 5) Writable temp locations
    def test_writable(path):
        try:
            # test creating and writing a small file
            os.makedirs(path, exist_ok=True)
            testfile = os.path.join(path, "airflow_write_test.txt")
            with open(testfile, "w") as f:
                f.write("ok")
            os.remove(testfile)
            return True
        except Exception as e:
            log.warning("write test failed at %s: %r", path, e)
            return False

    log.info("== Writable paths ==")
    paths_to_test = [
        "/tmp",
        "/home/airflow/gcs/data",
        "/home/airflow/gcs/dags",
    ]
    for p in paths_to_test:
        ok = test_writable(p)
        log.info("writable %-28s : %s", p, ok)

    # 6) Basic network test to github.com:443
    log.info("== Network test to github.com:443 ==")
    try:
        with socket.create_connection(("github.com", 443), timeout=5):
            log.info("TCP connect to github.com:443 OK")
    except Exception as e:
        log.warning("TCP connect to github.com:443 FAILED: %r", e)

    # 7) Try ls-remote with best-guess CA (certifi if available)
    remote = env.get("GIT_TEST_REMOTE", "https://github.com/git/git.git")
    if git_path:
        try:
            run_env = env.copy()
            if certifi_path:
                run_env["GIT_SSL_CAINFO"] = certifi_path
                log.info("Using GIT_SSL_CAINFO=%s", certifi_path)
            cmd = [git_path, "ls-remote", "--heads", remote]
            log.info("Running: %s", " ".join(cmd))
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, env=run_env, timeout=30)
            # log only first few lines to avoid noisy logs
            lines = out.strip().splitlines()
            log.info("git ls-remote succeeded. First lines:\n%s", "\n".join(lines[:5]) or "(no output)")
        except subprocess.CalledProcessError as e:
            log.warning("git ls-remote returned non-zero. Output:\n%s", e.output)
        except FileNotFoundError:
            log.warning("git not found during ls-remote run")
        except Exception as e:
            log.warning("git ls-remote error: %r", e)
    else:
        log.info("Skipping ls-remote because git is not installed.")

with DAG(
    dag_id="diagnose_git_tls_env",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    doc_md="""
This DAG inspects the Composer worker for:
- git availability and version  
- proxy env vars  
- SSL/CA locations (OpenSSL + certifi)  
- writability of /tmp and GCS-mounted paths  
- basic network to github.com  
- a test `git ls-remote` using GIT_SSL_CAINFO=certifi.where()
""",
) as dag:
    PythonOperator(
        task_id="diagnose",
        python_callable=diagnose_git_tls_env,
        provide_context=True,
    )
