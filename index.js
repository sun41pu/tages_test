const fs = require('fs');
const readline = require('readline');

let files = [];

//Алгоритм быстрой сортировки

function quickSortByLength(arr) {
  if (arr.length <= 1)
    return arr;
  let p = Math.floor(arr.length / 2), pivot = arr[p];
  const left = [], right = [];
  for (var i = 0; i < arr.length; i++) {
    if (i == p) continue;
    if (arr[i].length < pivot.length) {
      left.push(arr[i]);
    }
    else {
      right.push(arr[i]);
    }
  }
  return quickSortByLength(left).concat(pivot, quickSortByLength(right));
}

function merge(left, right) {
  let result = [];
  let leftIndex = 0;
  let rightIndex = 0;

  while (leftIndex < left.length && rightIndex < right.length) {
    if (left[leftIndex].length < right[rightIndex].length) {
      result.push(left[leftIndex]);
      leftIndex++;
    } else {
      result.push(right[rightIndex]);
      rightIndex++;
    }
  }

  return result.concat(left.slice(leftIndex), right.slice(rightIndex));
}

function mergeSortByLength(arr) {
  if (arr.length <= 1) {
    return arr;
  }

  const middle = Math.floor(arr.length / 2);
  const left = arr.slice(0, middle);
  const right = arr.slice(middle);

  return merge(mergeSortByLength(left), mergeSortByLength(right));
}

//Основная функция
async function sortLargeFile(inputFilePath) {
  const chunkSize = 250 * 1024 * 1024; // Устанавливаем размер чанка
  let chunkNumber = 0;
  const lineMap = new Map();
  console.log(chunkSize);
  
  const readStream = fs.createReadStream(inputFilePath, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: readStream, crlfDelay: Infinity });
  
  

  for await (const line of rl) {
    if (!lineMap.has(chunkNumber)) {
      lineMap.set(chunkNumber, []);
    }
    lineMap.get(chunkNumber).push(line);
    
    if (Buffer.from(line).length > chunkSize) {
      throw new Error('Строка превышает размер чанка');
    }


    if (Buffer.byteLength(lineMap.get(chunkNumber).join('\n'), 'utf8') > chunkSize) {
      const sortedChunk = quickSortByLength([...lineMap.get(chunkNumber)]);
      fs.writeFileSync(`chunk${chunkNumber}.txt`, sortedChunk.join('\n'));
      chunkNumber++;
      lineMap.set(chunkNumber, []);
    }
  }

  //Завершаем сортировку оставшихся строк
  for (let [number, lines] of lineMap.entries()) {
    const sortedChunk = quickSortByLength([...lines])//[...lines].sort((a, b) => a.length - b.length);
    files.push(`chunk${number}.txt`)
    fs.writeFileSync(`chunk${number}.txt`, sortedChunk.join('\n'));
  }
  
  // TODO: Объединить чанки обратно в один файл с помощью merge-sort

  const sortedFiles = files.map((file) => {
    const content = fs.readFileSync(file, 'utf8').split('\n');
    return mergeSortByLength(content);
  });

  const mergedContent = mergeSortByLength(sortedFiles.flat());
  for (let file of files) {
    fs.unlinkSync(file);
  }
  fs.writeFileSync('output_fin.txt', mergedContent.join('\n'));
  
}

sortLargeFile('input.txt');