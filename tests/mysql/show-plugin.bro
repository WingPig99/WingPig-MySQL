# @TEST-EXEC: bro -NN WingPig::MySQL |sed -e 's/version.*)/version)/g' >output
# @TEST-EXEC: btest-diff output
