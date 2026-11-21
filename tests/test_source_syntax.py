"""Shared input recognition, including boundaries that must stay rejected."""
import unittest

from Code2DAG.pipeline.source_syntax import (
    direct_thread_calls, mask_supported_enum_typedefs, SourceSyntaxError,
)
from Code2DAG.pipeline.composition import select_rule
from Code2DAG.pipeline.composition_model import CompositionError


class SourceSyntaxTests(unittest.TestCase):
    def test_enum_mask_preserves_offsets_comments_and_unrelated_text(self):
        text = '/* typedef int bad; */\ntypedef enum State { STOP = -1, INIT = 0x2, START, } state_t;\nstate_t state = INIT;\n'
        masked = mask_supported_enum_typedefs(text)
        self.assertEqual(len(text), len(masked))
        self.assertEqual([i for i,c in enumerate(text) if c=='\n'],
                         [i for i,c in enumerate(masked) if c=='\n'])
        self.assertEqual(text.splitlines()[0], masked.splitlines()[0])
        self.assertFalse(masked.splitlines()[1].strip())
        self.assertEqual(text.splitlines()[2], masked.splitlines()[2])

    def test_pointer_sync_local_and_complex_typedefs_remain_visible(self):
        for text in [
            'typedef pthread_mutex_t lock_t;', 'typedef int (*callback_t)(void);',
            'typedef enum { A } *state_t;', 'typedef enum { A = call() } state_t;',
            'typedef enum { A = 1 << 2 } state_t;', 'typedef enum { A } pthread_mutex_t;',
            'void f(void) { typedef enum { A } state_t; }',
        ]:
            with self.subTest(text=text):
                self.assertIn('typedef', mask_supported_enum_typedefs(text))

    def test_null_spellings_and_comment_sites(self):
        text = '// pthread_create(&bad, NULL, f, NULL);\n'
        text += 'pthread_create(&a, 0, worker, NULL);\npthread_join(a, 0);\n'
        calls = direct_thread_calls(text, 'create', standalone=True)
        self.assertEqual([('a','worker')], [(c.handle,c.entry) for c in calls])
        self.assertEqual('pthread_create(&a, 0, worker, NULL)', text[calls[0].start:calls[0].end])
        self.assertEqual('a', direct_thread_calls(text,'join',standalone=True)[0].handle)

    def test_unsupported_create_not_silently_dropped(self):
        valid = 'pthread_create(&a, NULL, one, NULL);\n'
        for call in ['pthread_create(&b, &attr, two, NULL);',
                     'pthread_create(&b, NULL, &two, NULL);',
                     'pthread_create(&b, NULL, two, &mutex);',
                     'pthread_create(&ts[i], NULL, two, NULL);']:
            with self.subTest(call=call), self.assertRaises(SourceSyntaxError):
                direct_thread_calls(valid+call, 'create')

    def test_standalone_policy_does_not_accept_wrappers(self):
        for text in ['CHECK(pthread_create(&a, 0, worker, 0));',
                     'rc = pthread_create(&a, 0, worker, 0);',
                     'pthread_create(&a, 0, worker, 0); other();']:
            with self.subTest(text=text), self.assertRaises(SourceSyntaxError):
                direct_thread_calls(text,'create',standalone=True)
        # The existing strict lock analyzer already accepted a single standalone
        # call whose arguments span lines; preserve that source-analysis contract.
        self.assertEqual('worker',direct_thread_calls(
            'pthread_create(\n&a, 0, worker, 0);','create',standalone=True)[0].entry)

    def test_c2b_detects_an_unrecognized_extra_create_and_reused_handle(self):
        from Code2DAG.tests.test_composition_c2b import SOURCES
        text = (SOURCES/'basic.c').read_text()
        for extra in ['pthread_create(&other, NULL, worker, &m);',
                      'pthread_create(&t, 0, worker2, 0);']:
            changed = text.replace('    pthread_join(', '    '+extra+'\n    pthread_join(', 1)
            with self.subTest(extra=extra), self.assertRaises(CompositionError):
                select_rule(changed)


if __name__ == '__main__':
    unittest.main()
