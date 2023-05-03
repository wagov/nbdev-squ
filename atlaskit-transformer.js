import { MarkdownTransformer } from '@atlaskit/editor-markdown-transformer';
import { JSONTransformer } from '@atlaskit/editor-json-transformer';
import { WikiMarkupTransformer } from '@atlaskit/editor-wikimarkup-transformer';

const transformers = {
    "md": new MarkdownTransformer(),
    "adf": new JSONTransformer(),
    "wiki": new WikiMarkupTransformer()
}

const readline = require('readline');

let args = process.argv.slice(2);

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

let input = '';

rl.on('line', (line) => {
  input += line + "\n";
});

rl.on('close', () => {
    let output = transformers[args[1]].encode(transformers[args[0]].parse(input));
    if (typeof output !== 'string') {
      output = JSON.stringify(output);
    }
    console.log(output);
});
