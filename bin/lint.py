import logging
from pylint.lint import Run
import click

logging.getLogger().setLevel(logging.INFO)


@click.command(name="LINT")
@click.option('--path',
              help='path to directory you want to run pylint | '
                   'Default: %(default)s | '
                   'Type: %(type)s ',
              default='./falcon')
@click.option('--threshold',
              help='score threshold to fail pylint runner | '
                   'Default: %(default)s | '
                   'Type: %(type)s ',
              default=7)

def run_lint(path, threshold):

    logging.info('PyLint Starting | '
                 'Path: {} | '
                 'Threshold: {} '.format(path, threshold))

    results = Run([path], do_exit=False)

    final_score = results.linter.stats['global_note']

    if final_score < threshold:

        message = ('PyLint Failed | '
                   'Score: {} | '
                   'Threshold: {} '.format(final_score, threshold))

        logging.error(message)
        raise Exception(message)

    else:
        message = ('PyLint Passed | '
                   'Score: {} | '
                   'Threshold: {} '.format(final_score, threshold))

        logging.info(message)

        exit(0)


if __name__ == '__main__':
    run_lint()
