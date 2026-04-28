from feature_engineer import run_feature_engineering
from feature_cleaning import run_cleaning
from feature_preprocessing import run_preprocessing


def main():

    print("\nFEATURE ENGINEERING")
    run_feature_engineering()

    print("\nFEATURE CLEANING")
    run_cleaning()

    print("\nPREPROCESSING")
    ml_data = run_preprocessing()
    ml_data.show(1, truncate=False)

    print("\nPIPELINE DONE")


if __name__ == "__main__":
    main()