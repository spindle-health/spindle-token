import os
from pathlib import Path
import pandas as pd
from click.testing import CliRunner
from pyspark.errors import AnalysisException
from carduus.cli import cli


class TestTokenizeCommand:
    pii = pd.DataFrame(
        {
            "first_name": pd.Series(["Louis", "louis"]),
            "last_name": pd.Series(["Pasteur", "pasteur"]),
            "gender": pd.Series(["male", "M"]),
            "birth_date": pd.Series(["1822-12-27", "1822-12-27"]),
        }
    )
    expected = pd.DataFrame(
        {
            "first_name": pd.Series(["LOUIS", "LOUIS"]),
            "last_name": pd.Series(["PASTEUR", "PASTEUR"]),
            "gender": pd.Series(["M", "M"]),
            "birth_date": pd.Series(["1822-12-27", "1822-12-27"]),
            "opprl_token_1": pd.Series(
                [
                    "NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                    "NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                ]
            ),
            "opprl_token_3": pd.Series(
                [
                    "op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                    "op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                ]
            ),
        }
    )

    def test_option_names(self, tmp_path: Path, private_key: bytes):
        runner = CliRunner()
        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        self.pii.to_csv(tmp_path / "pii.csv", sep="|", header=True, index=False)
        result = runner.invoke(
            cli,
            [
                "tokenize",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                "--key",
                str(tmp_path / "test_key.pem"),
                "--format",
                "csv",
                str(tmp_path / "pii.csv"),
                str(tmp_path / "tokens.csv"),
            ],
        )
        assert result.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == ["pii.csv", "test_key.pem", "tokens.csv"]
        actual = pd.read_csv(tmp_path / "tokens.csv", sep="|", header=0)
        pd.testing.assert_frame_equal(actual, self.expected)

    def test_option_alias(self, tmp_path: Path, private_key: bytes):
        runner = CliRunner()
        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        self.pii.to_csv(tmp_path / "pii.csv", sep="|", header=True, index=False)
        result = runner.invoke(
            cli,
            [
                "tokenize",
                "-t",
                "opprl_token_1",
                "-t",
                "opprl_token_3",
                "-k",
                str(tmp_path / "test_key.pem"),
                "-f",
                "csv",
                str(tmp_path / "pii.csv"),
                str(tmp_path / "tokens.csv"),
            ],
        )
        assert result.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == ["pii.csv", "test_key.pem", "tokens.csv"]
        actual = pd.read_csv(tmp_path / "tokens.csv", sep="|", header=0)
        pd.testing.assert_frame_equal(actual, self.expected)

    def test_env_vars(self, tmp_path: Path, private_key: bytes):
        runner = CliRunner()
        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        self.pii.to_csv(tmp_path / "pii.csv", sep="|", header=True, index=False)
        result = runner.invoke(
            cli,
            [
                "tokenize",
                "-t",
                "opprl_token_1",
                "-t",
                "opprl_token_3",
                str(tmp_path / "pii.csv"),
                str(tmp_path / "tokens.csv"),
            ],
            env=dict(
                SPINDLE_TOKEN_PRIVATE_KEY=str(tmp_path / "test_key.pem"),
                SPINDLE_TOKEN_FORMAT="csv",
                SPINDLE_TOKEN_PARALLELISM="1",
            ),
        )
        assert result.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == ["pii.csv", "test_key.pem", "tokens.csv"]
        actual = pd.read_csv(tmp_path / "tokens.csv", sep="|", header=0)
        pd.testing.assert_frame_equal(actual, self.expected)

    def test_invalid_key(self, tmp_path: Path, acme_public_key: bytes):
        runner = CliRunner()
        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(acme_public_key)
        self.pii.to_csv(tmp_path / "pii.csv", sep="|", header=True, index=False)
        result = runner.invoke(
            cli,
            [
                "tokenize",
                "-t",
                "opprl_token_1",
                "-t",
                "opprl_token_3",
                "-k",
                str(tmp_path / "test_key.pem"),
                "-f",
                "csv",
                str(tmp_path / "pii.csv"),
                str(tmp_path / "tokens.csv"),
            ],
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, ValueError)
        assert sorted(os.listdir(tmp_path)) == ["pii.csv", "test_key.pem"]

    def test_missing_pii(self, tmp_path: Path, private_key: bytes, spark):
        runner = CliRunner()
        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        self.pii.drop(columns=["last_name"]).to_csv(
            tmp_path / "pii.csv", sep="|", header=True, index=False
        )
        result = runner.invoke(
            cli,
            [
                "tokenize",
                "-t",
                "opprl_token_1",
                "-t",
                "opprl_token_3",
                "-k",
                str(tmp_path / "test_key.pem"),
                "-f",
                "csv",
                str(tmp_path / "pii.csv"),
                str(tmp_path / "tokens.csv"),
            ],
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, AnalysisException)
        assert sorted(os.listdir(tmp_path)) == ["pii.csv", "test_key.pem"]

    def test_multipart_files_and_no_parallelism(self, tmp_path: Path, private_key: bytes):
        runner = CliRunner()
        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        os.mkdir(tmp_path / "pii")
        self.pii.iloc[[0]].to_csv(tmp_path / "pii/part0.csv", sep="|", header=True, index=False)
        self.pii.iloc[[1]].to_csv(tmp_path / "pii/part1.csv", sep="|", header=True, index=False)
        result = runner.invoke(
            cli,
            [
                "tokenize",
                "-t",
                "opprl_token_1",
                "-t",
                "opprl_token_3",
                "-k",
                str(tmp_path / "test_key.pem"),
                "-f",
                "csv",
                str(tmp_path / "pii"),
                str(tmp_path / "tokens"),
            ],
        )
        assert result.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == ["pii", "test_key.pem", "tokens"]
        # 2 csv part files, a _SUCCESS file, and a checksum for each.
        assert len(os.listdir(tmp_path / "tokens")) == 6


class TestTranscryptCommands:
    tokens = pd.DataFrame(
        {
            "opprl_token_1": pd.Series(
                [
                    "NJQZ0hNk40pt5aFitlwdx6k2Te7hMSMw1UHzNdgP1aUYbqaFbGSe3tRn4kL/OxsFJit9VhRDYRawUDYzKivYlLm2EzKCO0+nC9rJXfIFwdo=",
                ]
            ),
            "opprl_token_3": pd.Series(
                [
                    "op48PUlod5WC7PMQHJp1wEtAApfgu/1G2WuGoVDMQ6V5EZPI7X5BfpZzrdoOrA90CFVw3X79t0ygazbSFrRKx+SupOvBYFRr8ZRZmT5oz8k=",
                ]
            ),
        }
    )
    expected = pd.DataFrame(
        {
            "opprl_token_1": pd.Series(
                [
                    "U/JYKVLQWSUrpvJ1D03pvKmnhlgUTFjHaPtS0pZBLSqrDCOkBOR/mDf9xFt/Cr3AB8hI00oEkuunCTvNV3zbgdz9Y0jcwiI16zn51jSkhhM=",
                ]
            ),
            "opprl_token_3": pd.Series(
                [
                    "cOrhMGV6oO3Vt8w3vV1K4TzvNYlkZZ9JOj9/53IGkD7vgce0I13uOrDFCcJEXD1qEa4Mm1Nimq4sprd8tFrdDHRDCOeZBE2Gs4DEEt7LhL0=",
                ]
            ),
        }
    )

    def test_option_names(
        self,
        tmp_path: Path,
        private_key: bytes,
        acme_public_key: bytes,
        acme_private_key: bytes,
    ):
        runner = CliRunner()

        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        with open(tmp_path / "acme_pubkey.pem", "wb") as f:
            f.write(acme_public_key)
        with open(tmp_path / "acme_key.pem", "wb") as f:
            f.write(acme_private_key)
        self.tokens.to_csv(tmp_path / "tokens.csv", sep="|", header=True, index=False)

        result1 = runner.invoke(
            cli,
            [
                "transcrypt",
                "out",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                "--key",
                str(tmp_path / "test_key.pem"),
                "--recipient",
                str(tmp_path / "acme_pubkey.pem"),
                "--format",
                "csv",
                str(tmp_path / "tokens.csv"),
                str(tmp_path / "ephemeral_tokens.csv"),
            ],
        )
        assert result1.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "ephemeral_tokens.csv",
            "test_key.pem",
            "tokens.csv",
        ]

        result2 = runner.invoke(
            cli,
            [
                "transcrypt",
                "in",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                "--key",
                str(tmp_path / "acme_key.pem"),
                "--format",
                "csv",
                str(tmp_path / "ephemeral_tokens.csv"),
                str(tmp_path / "acme_tokens.csv"),
            ],
        )
        assert result2.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "acme_tokens.csv",
            "ephemeral_tokens.csv",
            "test_key.pem",
            "tokens.csv",
        ]

        actual = pd.read_csv(tmp_path / "acme_tokens.csv", sep="|", header=0)
        pd.testing.assert_frame_equal(actual, self.expected)

    def test_option_aliases(
        self,
        tmp_path: Path,
        private_key: bytes,
        acme_public_key: bytes,
        acme_private_key: bytes,
    ):
        runner = CliRunner()

        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        with open(tmp_path / "acme_pubkey.pem", "wb") as f:
            f.write(acme_public_key)
        with open(tmp_path / "acme_key.pem", "wb") as f:
            f.write(acme_private_key)
        self.tokens.to_csv(tmp_path / "tokens.csv", sep="|", header=True, index=False)

        result1 = runner.invoke(
            cli,
            [
                "transcrypt",
                "out",
                "-t",
                "opprl_token_1",
                "-t",
                "opprl_token_3",
                "-k",
                str(tmp_path / "test_key.pem"),
                "-r",
                str(tmp_path / "acme_pubkey.pem"),
                "-f",
                "csv",
                str(tmp_path / "tokens.csv"),
                str(tmp_path / "ephemeral_tokens.csv"),
            ],
        )
        assert result1.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "ephemeral_tokens.csv",
            "test_key.pem",
            "tokens.csv",
        ]

        result2 = runner.invoke(
            cli,
            [
                "transcrypt",
                "in",
                "-t",
                "opprl_token_1",
                "-t",
                "opprl_token_3",
                "-k",
                str(tmp_path / "acme_key.pem"),
                "-f",
                "csv",
                str(tmp_path / "ephemeral_tokens.csv"),
                str(tmp_path / "acme_tokens.csv"),
            ],
        )
        assert result2.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "acme_tokens.csv",
            "ephemeral_tokens.csv",
            "test_key.pem",
            "tokens.csv",
        ]

        actual = pd.read_csv(tmp_path / "acme_tokens.csv", sep="|", header=0)
        pd.testing.assert_frame_equal(actual, self.expected)

    def test_env_vars(
        self,
        tmp_path: Path,
        private_key: bytes,
        acme_public_key: bytes,
        acme_private_key: bytes,
    ):
        runner = CliRunner()

        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        with open(tmp_path / "acme_pubkey.pem", "wb") as f:
            f.write(acme_public_key)
        with open(tmp_path / "acme_key.pem", "wb") as f:
            f.write(acme_private_key)
        self.tokens.to_csv(tmp_path / "tokens.csv", sep="|", header=True, index=False)

        result1 = runner.invoke(
            cli,
            [
                "transcrypt",
                "out",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                str(tmp_path / "tokens.csv"),
                str(tmp_path / "ephemeral_tokens.csv"),
            ],
            env=dict(
                SPINDLE_TOKEN_PRIVATE_KEY=str(tmp_path / "test_key.pem"),
                SPINDLE_TOKEN_RECIPIENT_PUBLIC_KEY=str(tmp_path / "acme_pubkey.pem"),
                SPINDLE_TOKEN_FORMAT="csv",
            ),
        )
        assert result1.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "ephemeral_tokens.csv",
            "test_key.pem",
            "tokens.csv",
        ]

        result2 = runner.invoke(
            cli,
            [
                "transcrypt",
                "in",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                str(tmp_path / "ephemeral_tokens.csv"),
                str(tmp_path / "acme_tokens.csv"),
            ],
            env=dict(
                SPINDLE_TOKEN_PRIVATE_KEY=str(tmp_path / "acme_key.pem"),
                SPINDLE_TOKEN_FORMAT="csv",
            ),
        )
        assert result2.exit_code == 0
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "acme_tokens.csv",
            "ephemeral_tokens.csv",
            "test_key.pem",
            "tokens.csv",
        ]

        actual = pd.read_csv(tmp_path / "acme_tokens.csv", sep="|", header=0)
        pd.testing.assert_frame_equal(actual, self.expected)

    def test_invalid_pubkey(
        self,
        tmp_path: Path,
        private_key: bytes,
        acme_public_key: bytes,
        acme_private_key: bytes,
    ):
        runner = CliRunner()

        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        self.tokens.to_csv(tmp_path / "tokens.csv", sep="|", header=True, index=False)

        result = runner.invoke(
            cli,
            [
                "transcrypt",
                "out",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                "--key",
                str(tmp_path / "test_key.pem"),
                "--recipient",
                str(tmp_path / "test_key.pem"),
                "--format",
                "csv",
                str(tmp_path / "tokens.csv"),
                str(tmp_path / "ephemeral_tokens.csv"),
            ],
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, ValueError)
        assert sorted(os.listdir(tmp_path)) == ["test_key.pem", "tokens.csv"]

    def test_missing_tokens(
        self,
        tmp_path: Path,
        private_key: bytes,
        acme_public_key: bytes,
        acme_private_key: bytes,
    ):
        runner = CliRunner()

        with open(tmp_path / "test_key.pem", "wb") as f:
            f.write(private_key)
        with open(tmp_path / "acme_pubkey.pem", "wb") as f:
            f.write(acme_public_key)
        with open(tmp_path / "acme_key.pem", "wb") as f:
            f.write(acme_private_key)
        self.tokens.drop(columns=["opprl_token_3"]).to_csv(
            tmp_path / "tokens.csv", sep="|", header=True, index=False
        )

        result1 = runner.invoke(
            cli,
            [
                "transcrypt",
                "out",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                "--key",
                str(tmp_path / "test_key.pem"),
                "--recipient",
                str(tmp_path / "acme_pubkey.pem"),
                "--format",
                "csv",
                str(tmp_path / "tokens.csv"),
                str(tmp_path / "ephemeral_tokens.csv"),
            ],
        )
        assert result1.exit_code == 1
        assert isinstance(result1.exception, AnalysisException)
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "test_key.pem",
            "tokens.csv",
        ]

        # Create ephemeral tokens correctly to test `transcrypt in`
        self.tokens.to_csv(tmp_path / "tokens.csv", sep="|", header=True, index=False)
        runner.invoke(
            cli,
            [
                "transcrypt",
                "out",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                "--key",
                str(tmp_path / "test_key.pem"),
                "--recipient",
                str(tmp_path / "acme_pubkey.pem"),
                "--format",
                "csv",
                str(tmp_path / "tokens.csv"),
                str(tmp_path / "ephemeral_tokens.csv"),
            ],
        )

        # Remove one of the ephemeral token columns.
        (
            pd.read_csv(tmp_path / "ephemeral_tokens.csv", sep="|", header=0)
            .drop(columns=["opprl_token_3"])
            .to_csv(tmp_path / "ephemeral_tokens.csv", sep="|", header=True, index=False)
        )

        result2 = runner.invoke(
            cli,
            [
                "transcrypt",
                "in",
                "--token",
                "opprl_token_1",
                "--token",
                "opprl_token_3",
                "--key",
                str(tmp_path / "acme_key.pem"),
                "--format",
                "csv",
                str(tmp_path / "ephemeral_tokens.csv"),
                str(tmp_path / "acme_tokens.csv"),
            ],
        )
        assert result2.exit_code == 1
        assert isinstance(result2.exception, AnalysisException)
        assert sorted(os.listdir(tmp_path)) == [
            "acme_key.pem",
            "acme_pubkey.pem",
            "ephemeral_tokens.csv",
            "test_key.pem",
            "tokens.csv",
        ]
