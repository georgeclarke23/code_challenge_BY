class TestContext:
    def test_should_get_variable_from_env(self, monkeypatch, fake_context):
        expected = "test"
        monkeypatch.setenv("SOURCE_PATH", expected)
        actual = fake_context.get("SOURCE_PATH")
        assert actual == expected

    def test_should_get_variable_from_local_state(self, fake_context):
        expected = "test"
        fake_context.set({"SOURCE_PATH": expected})
        actual = fake_context.get("SOURCE_PATH")
        assert actual == expected

    def test_should_set_variable_to_local_state(self, fake_context):
        expected = "test"
        fake_context.set({"test": expected})
        actual = fake_context.get("test")
        assert actual == expected
