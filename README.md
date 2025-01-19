# Complete Windows Setup for Java, Hadoop, Spark, and Python (PySpark)

Below is a **comprehensive** step‐by‐step guide for installing and configuring Java, Hadoop, Spark, and Python on **Windows**. It also covers environment variables, folder permissions, and common pitfalls. **RECOMMENDED:** If you want to save time please look at the **docker_setup.md**, the process there is a lot more straightforward. The docker guide doubles as a setup for linux since the process is kinda the same on linux.

---

## 1. Overview

We’ll install and configure:
1. **Java (JDK)**
2. **Hadoop** (with `winutils.exe`)
3. **Spark** (prebuilt with Hadoop)
4. **Python** (preferably 3.9 or 3.10)
5. **Environment Variables** for each
6. **Permissions and Security** considerations (to avoid “Access is denied”)
7. **Common Errors & Solutions**

**Goal**: Be able to run a command like:
spark-submit src\main.py

…and successfully execute PySpark code using a local Spark “standalone” environment on Windows.

> **Note**: All steps below assume you are on **Windows 10 or later**, with Administrator privileges.

---

## 2. Install Java (JDK)

1. **Download** a Java 8+ (or Java 11 or 17—Spark can work with multiple JDK versions).  
   - For example, from [Adoptium (Temurin)](https://adoptium.net/) or Oracle’s site.  

2. **Install** it to a location such as:
C:\Java\jdk-17

3. **Set** the `JAVA_HOME` environment variable. You can do this via *System Properties → Environment Variables* or a command prompt:
setx JAVA_HOME "C:\Java\jdk-17"

4. **Add** `%JAVA_HOME%\bin` to your system `PATH` if needed:
setx PATH "%PATH%;%JAVA_HOME%\bin"

5. **Verify** in a new Command Prompt:
java -version

It should display something like `openjdk version "17.x"`.

---

## 3. Install Hadoop (3.x) + `winutils.exe`

Even if you only plan to run Spark locally, Spark uses some Hadoop file system utilities. On Windows, you need the `winutils.exe` binary to handle certain file operations.

1. **Download** a Hadoop 3.x binary distribution. For instance, [Apache Hadoop 3.3.0 or later](https://hadoop.apache.org/releases.html).

2. **Unzip** it to:
C:\hadoop-3.3.0

Inside there should be a `bin` folder containing `winutils.exe`. If `winutils.exe` is missing, download a matching version from a known “winutils” repository.

3. **Set** `HADOOP_HOME` to the parent folder (not the `bin`):
setx HADOOP_HOME "C:\hadoop-3.3.0"
**Important**: Do **not** append `\bin` to `HADOOP_HOME`. Spark will do that internally.

4. **Add** `%HADOOP_HOME%\bin` to your system `PATH`:
setx PATH "%PATH%;%HADOOP_HOME%\bin"

5. **Verify** in a new Admin Command Prompt:
where winutils

You should see:
C:\hadoop-3.3.0\bin\winutils.exe


> **Common Pitfall**: If you set `HADOOP_HOME=C:\hadoop-3.3.0\bin`, Spark tries to append another `\bin`, resulting in `C:\hadoop-3.3.0\bin\bin\winutils.exe` which does not exist.

---

## 4. Install Spark (Prebuilt with Hadoop)

1. **Download** a Spark build “prebuilt for Hadoop 3.2 or 3.3” from the [Spark downloads page](https://spark.apache.org/downloads.html).  
Example:  
spark-3.4.0-bin-hadoop3.tgz

2. **Unzip** it to:
C:\spark-3.4.0-bin-hadoop3

3. **Set** `SPARK_HOME`:
setx SPARK_HOME "C:\spark-3.4.0-bin-hadoop3"

4. **Add** `%SPARK_HOME%\bin` to your PATH:
setx PATH "%PATH%;%SPARK_HOME%\bin"


5. **Verify** in a new terminal:
spark-shell --version

You should see Spark’s version. If you get “HADOOP_HOME not found,” check your `HADOOP_HOME`. If you see “winutils.exe not found,” ensure `winutils.exe` is in `%HADOOP_HOME%\bin`.

---

## 5. Install Python 3.9 or 3.10

1. **Download** Python from [python.org](https://www.python.org/downloads/).  

2. During installation, **uncheck** “Install for only me” if possible, or otherwise ensure it’s installed in a location without special user permissions. Ideally:
C:\Python310
rather than something in `AppData\Local\Programs`.

3. **Add** Python to your PATH (or check the “Add Python to PATH” box during install).

4. **Verify**:
python --version

Should display `Python 3.10.x`.

> **Tip**: If you prefer Conda/Miniconda, that’s fine. Just ensure your environment variables point to `C:\Miniconda3\envs\spark\python.exe` or similar.

---

## 6. Configure PySpark Environment Variables

Spark, when using PySpark, needs to know which Python to use:

1. **Set** `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to your full Python path:


setx PYSPARK_PYTHON "C:\Python310\python.exe" setx PYSPARK_DRIVER_PYTHON "C:\Python310\python.exe"

2. **Open** a new terminal (Admin CMD or PowerShell):
echo %PYSPARK_PYTHON% echo %PYSPARK_DRIVER_PYTHON%

Both should show `C:\Python310\python.exe`.

---

## 7. Permissions and “Access is Denied” Issues

### 7.1 Run Terminal as Administrator

- Sometimes Windows will block launching processes from certain folders.  
- Right‐click “Command Prompt” or “PowerShell” → **Run as administrator**.  
- Then try:
spark-submit src\main.py


### 7.2 Checking File Security with `icacls`

If you **still** get “CreateProcess error=5, Access is denied” for `python.exe`:

1. Right‐click `C:\Python310\python.exe` → **Properties** → **Security**. Ensure your user has “Read & execute” or “Full control.”
2. In an **admin** CMD, you can force it:
icacls "C:\Python310\python.exe" /grant Everyone:(F)


(Granting Full control to everyone is a bit broad, but will test if it’s purely a file permission issue.)

### 7.3 Antivirus / Windows Defender

- Sometimes corporate AV or Windows Defender blocks `.exe` files in certain folders.  
- Temporarily **disable** your AV or **whitelist** your Python/Spark folders to see if the error goes away.

---

## 8. Common Errors and Solutions

1. **`java.io.FileNotFoundException: Hadoop bin directory does not exist...`**  
- Typically means `HADOOP_HOME` is set to `C:\hadoop-3.3.0\bin` instead of `C:\hadoop-3.3.0`.  
- Correct your `HADOOP_HOME` and ensure `%HADOOP_HOME%\bin` is on PATH.

2. **`Missing Python executable 'python3', defaulting to...`**  
- Spark tries to find “python3” on Windows.  
- Fix: Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON`.

3. **`CreateProcess error=5, Access is denied`**  
- Windows is outright blocking the Python `.exe`.  
- Fix: Move Python out of `AppData` to `C:\Python310`, run as admin, or set file ACLs via `icacls`.

4. **`winutils.exe not found`**  
- Ensure `winutils.exe` is in `%HADOOP_HOME%\bin`. If missing, get a matching version from a “winutils” GitHub repo.  

5. **CSV or Data Path Not Found**  
- `[PATH_NOT_FOUND] Path does not exist: ...`  
- Ensure your script references the correct file name and path. Also confirm the CSV is physically there.

6. **NativeCodeLoader: “Unable to load native-hadoop library...”**  
- Usually just a **warning**. Spark can still run using built-in Java classes.

---

## 9. Final Verification

After completing all the above steps, in an **administrator** Command Prompt:

1. Check environment variables:
echo %JAVA_HOME% echo %HADOOP_HOME% echo %SPARK_HOME% echo %PYSPARK_PYTHON% where winutils where spark-submit where python

2. Navigate to your project folder:
cd C:\path-to-your-project-root

3. Run a quick test:
spark-submit src\main.py

or an interactive PySpark shell:
pyspark

If successful, you can do:
df = spark.read.csv("data/file.csv", header=True, inferSchema=True) df.show()

to see if everything works.

---

## 10. Typical Usage

- **Running a PySpark script**:
spark-submit src\main.py

- **Interactive Shell**:
pyspark

Then in Python:
df = spark.read.csv("data/file.csv", header=True, inferSchema=True) df.show()


---

## 11. Additional Tips

- **Keep Spark/Hadoop/Python versions aligned**. For example, Spark 3.4 with Hadoop 3.x, Python 3.10.  
- **If behind corporate restrictions**, coordinate with IT to allow `.exe` files in certain folders.  
- **Conda** is an alternative if you prefer environment management. Just ensure you set `PYSPARK_PYTHON` to the Conda environment’s `python.exe`.

---

## 12. Conclusion

By following these **foolproof** steps, you should have:
- Java installed, with `JAVA_HOME` set
- Hadoop in `C:\hadoop-3.3.0`, with `winutils.exe` in `\bin`
- Spark in `C:\spark-3.4.0-bin-hadoop3`, with `SPARK_HOME`
- Python 3.9+ in `C:\Python310`, environment variables pointing to `python.exe`
- No “Access is denied” errors, no “winutils.exe not found,” and data files loaded successfully.

You can now develop PySpark applications, run `spark-submit` jobs, and enjoy a fully functional local Spark environment on Windows.

## 13. Running the spark job

in the project root run 

```
spark-submit src/main.py
```