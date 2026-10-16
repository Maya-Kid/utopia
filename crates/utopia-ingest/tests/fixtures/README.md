`sparse-sheets.xlsx` is a small, model-free OOXML workbook with two sparse sheets:

- `Tall`: A1 = `tall-header`, B2000 = `tall-last-read`, P1000000 = `tall-omitted`.
- `Wide`: A1 = `wide-header`, B200 = `wide-last-read`, ALL201 = `wide-omitted`.

Tall has an explicit worksheet dimension; Wide omits the optional hint. The
range test checks the retained dense grid dimensions.
The public parser test checks the retained content and complete row warnings.
Run both with `cargo test -p utopia-ingest sparse`.
