FOLDER=csc-369-student-instructor
rm -rf ../$FOLDER/*
cp requirements.txt ../$FOLDER/
cp install.sh ../$FOLDER/
cp common*.py ../$FOLDER/
cp -Rp data ../$FOLDER/
cp -Rp chapters ../$FOLDER/
cp -Rp labs ../$FOLDER/
cp -Rp src ../$FOLDER/
cp -Rp assignments ../$FOLDER/
cp -Rp groups ../$FOLDER/
cp -Rp tests ../$FOLDER/
cp -Rp config ../$FOLDER/
cp -Rp lectures ../$FOLDER/
cp -Rp project ../$FOLDER/
cp -Rp data ../$FOLDER/
cp -Rp exam_study_info ../$FOLDER/
find ../$FOLDER/ -name ".ipy*" -exec rm -rf {} \;
cp -Rp tutorials ../$FOLDER/

# TODO go through every md file and remove certain sections
# BEGIN SOLUTION
# END SOLUTION
SAVEIFS=$IFS
IFS=$(echo -en "\n\b")

for file in `find ../$FOLDER/chapters/ -name "*.py" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.py").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.py","w").write("\n".join(lines))
EOF
done;

for file in `find ../$FOLDER/chapters/ -name "*.md" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.md").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.md","w").write("\n".join(lines))
EOF
done;

for file in `find ../$FOLDER/chapters/ -name "*.ipynb" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.ipynb").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.ipynb","w").write("\n".join(lines))
EOF
done;


for file in `find ../$FOLDER/labs/ -name "*.py" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.py").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.py","w").write("\n".join(lines))
EOF
done;

for file in `find ../$FOLDER/labs/ -name "*.md" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.md").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.md","w").write("\n".join(lines))
EOF
done;

for file in `find ../$FOLDER/labs/ -name "*.ipynb" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.ipynb").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.ipynb","w").write("\n".join(lines))
EOF
done;

for file in `find ../$FOLDER/lectures/ -name "*.ipynb" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.ipynb").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.ipynb","w").write("\n".join(lines))
EOF
done;

for file in `find ../$FOLDER/assignments/ -name "*.py" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.py").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.py","w").write("\n".join(lines))
EOF
done;


for file in `find ../$FOLDER/assignments/ -name "*.md" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.md").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.md","w").write("\n".join(lines))
EOF
done;

for file in `find ../$FOLDER/assignments/ -name "*.ipynb" -print`; do
echo $file
filename="${file%.*}"
python - <<EOF
contents = open("$filename.ipynb").read()
lines = []
started_solution = False
for line in contents.split("\n"):
  if "BEGIN SOLUTION" in line:
    started_solution = True
  elif "END SOLUTION" in line:
    started_solution = False
  elif started_solution == False:
    lines.append(line)
open("$filename.ipynb","w").write("\n".join(lines))
EOF
done;

CURRENT=`pwd`
cd ../$FOLDER
git add .
git commit -m update
git push
cd $CURRENT
