# HOW TO USE GRANDPA JOE
## Horse Racing Handicapping Assistant
### (Written for someone who has never used a computer terminal before)

---

## STEP 1: OPEN THE TERMINAL

**On Windows:**
- Press the **Windows key** on your keyboard (bottom left, looks like a flag)
- Type **cmd** and press **Enter**
- A black window with white text will appear — this is your terminal

**On Mac:**
- Press **Command + Space** (opens Spotlight)
- Type **Terminal** and press **Enter**

---

## STEP 2: GO TO THE RIGHT FOLDER

You need to tell the computer where Grandpa Joe lives. Type this EXACTLY and press Enter:

```
cd C:\Users\danie\projects\GRANDPA_JOE
```

You should see the folder name appear in your terminal. If you see an error, make sure you typed it exactly right.

---

## STEP 3: WHAT YOU CAN DO

### See how much data Grandpa Joe knows:
```
python -m grandpa_joe stats
```
This shows you how many horses, races, and results are in his brain.

---

### Get picks for a race:
```
python -m grandpa_joe handicap SAR 5
```
This means: "Hey Grandpa Joe, handicap **Race 5** at **Saratoga (SAR)**"

Replace **SAR** with the track code and **5** with the race number.

**Common track codes:**
| Code | Track |
|------|-------|
| SAR | Saratoga |
| CD  | Churchill Downs |
| GP  | Gulfstream Park |
| AQU | Aqueduct |
| BEL | Belmont |
| SA  | Santa Anita |
| OP  | Oaklawn Park |
| KEE | Keeneland |
| PIM | Pimlico |
| DMR | Del Mar |

---

### Talk to Grandpa Joe:
```
python -m grandpa_joe chat
```
This opens a conversation with Grandpa Joe. Type your questions and he answers.
Type **quit** to leave.

---

### Train the model (make him smarter):
```
python -m grandpa_joe train
```
This takes about **45 minutes**. Only do this after adding new data.
You'll see progress updates as it works.

---

### Feed him new race data (CSV file):
```
python -m grandpa_joe ingest "C:\path\to\your\file.csv"
```
Replace the path with wherever your CSV file is saved.

---

### Feed him Equibase XML data:
```
python -m grandpa_joe ingest-xml "C:\path\to\chart.xml"
```

---

### Feed him a whole folder of data at once:
```
python -m grandpa_joe ingest-dir "C:\path\to\data\folder"
```
This scans the folder for all CSV, XML, and ZIP files and loads them all.

---

### Start the web server (for phone/browser access):
```
python -m grandpa_joe --server
```
Then open your web browser and go to: **http://localhost:8100**

---

## COMMON PROBLEMS

### "python is not recognized"
Python isn't installed or isn't in your PATH. Ask someone to install Python 3.11 or later.

### "No module named grandpa_joe"
You need to install it first. Run this ONE TIME:
```
pip install -e .
```

### "ML dependencies not installed"
Run this ONE TIME:
```
pip install -e ".[ml]"
```

### "No race found"
The track code or race number doesn't match anything in the database. Try `python -m grandpa_joe stats` to see what data is loaded.

### The terminal is frozen / nothing is happening
The model might be training or processing. Be patient. If you need to stop it, press **Ctrl + C** (hold Ctrl and press C).

---

## QUICK REFERENCE CARD

| What you want | What you type |
|---------------|---------------|
| Open terminal | Windows key, type `cmd`, Enter |
| Go to folder | `cd C:\Users\danie\projects\GRANDPA_JOE` |
| See stats | `python -m grandpa_joe stats` |
| Get picks | `python -m grandpa_joe handicap SAR 5` |
| Chat | `python -m grandpa_joe chat` |
| Train model | `python -m grandpa_joe train` |
| Load CSV data | `python -m grandpa_joe ingest "file.csv"` |
| Load XML data | `python -m grandpa_joe ingest-xml "file.xml"` |
| Load folder | `python -m grandpa_joe ingest-dir "folder"` |
| Web server | `python -m grandpa_joe --server` |
| Stop anything | **Ctrl + C** |
| Close terminal | Type `exit` and press Enter |

---

*Grandpa Joe - by Daniel J Rita / GxEum Technologies / CAMDAN Enterprizes*
