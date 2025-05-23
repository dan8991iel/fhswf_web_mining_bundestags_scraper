import json


def main():
    # Load existing counts
    with open('h2_counts.json', 'r', encoding='utf-8') as f:
        counts = json.load(f)

    # Sort the dictionary by count descending
    sorted_counts = dict(
        sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    )

    # Overwrite the JSON file with sorted counts
    with open('h2_counts.json', 'w', encoding='utf-8') as f:
        json.dump(sorted_counts, f, ensure_ascii=False, indent=2)

    print("h2_counts.json has been sorted and updated.")


if __name__ == '__main__':
    main()