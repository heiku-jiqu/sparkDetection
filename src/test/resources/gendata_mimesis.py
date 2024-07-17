from mimesis.enums import TimestampFormat
from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema

field = Field(locale=Locale.EN)
schema = Schema(
    schema=lambda: {
        "pk": field("increment"),
        "name": field("text.word", key=maybe("N/A", probability=0.2)),
        "version": field("version"),
        "timestamp": field("timestamp", TimestampFormat.RFC_3339),
    },
    iterations=1000
)
