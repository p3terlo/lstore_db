START=$(date +%s.%N)
python -m unittest tests/test_bufferpool.py
END=$(date +%s.%N)
DIFF=$(echo "$END - $START" | bc)

echo "Program takes "$DIFF "seconds to complete."