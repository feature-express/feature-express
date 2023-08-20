import glob

if __name__ == '__main__':
    for fn in glob.glob("*.rs"):
        with open(fn) as inp:
            contents = inp.read()

        if " PartialAggregate " in contents and " SubtractPartialAggregate " in contents:
            if "#[test]" not in contents:
                print("{} missing test".format(fn))
            else:
                print("{} complete".format(fn))