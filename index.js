const fs = require('fs');
const readline = require('readline');

let files = [];

//Алгоритм быстрой сортировки
function quickSortByLength(arr) {
  if (arr.length <= 1) {
    return arr;
  } else {
    const left = [];
    const right = [];
    const pivot = arr[0];
    for (let i = 1; i < arr.length; i++) {
      if (arr[i].length < pivot.length) {
        left.push(arr[i]);
      } else {
        right.push(arr[i]);
      }
    }
    return quickSortByLength(left).concat(pivot, quickSortByLength(right));
  }
}


//Основная функция
async function sortLargeFile(inputFilePath) {
  const chunkSize = 250 * 1024 * 1024; // Устанавливаем размер чанка
  let chunkNumber = 0;
  
  const lineMap = new Map();
  
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
    
    if (Buffer.from([...lineMap.get(chunkNumber)]).length > chunkSize) {

      // Сортируем и записываем чанк
      const sortedChunk = quickSortByLength([...lineMap.get(chunkNumber)])
      fs.writeFileSync(`chunk${chunkNumber}.txt`, sortedChunk.join('\n'));
      // Переходим к следующему чанку
      chunkNumber++;
    }
  }

  //Завершаем сортировку оставшихся строк
  for (let [number, lines] of lineMap.entries()) {
    const sortedChunk = quickSortByLength([...lines])//[...lines].sort((a, b) => a.length - b.length);
    files.push(`chunk${number}.txt`)
    fs.writeFileSync(`chunk${number}.txt`, sortedChunk.join('\n'));
  }
  
  // TODO: Объединить чанки обратно в один файл с помощью merge-sort

  function mergeFiles(fileList, outputFile) {
    let lists = [];
    for (let file of fileList) {
      let lines = fs.readFileSync(file, 'utf8').split('\n');
      lists.push(quickSortByLength(lines));
      // let lines = fs.readFileSync(file, 'utf8').split('\n');
      // lines.sort((a, b) => a.length - b.length);
      // lists.push(lines);
    }
  
    let mergedList = quickSortByLength([].concat(...lists))
    //удаляем временные файлы
    for (let file of files) {
      fs.unlinkSync(file);
    }
  
    fs.writeFileSync(outputFile, mergedList.join('\n'));
  }

  

  mergeFiles(files, 'sorted_out.txt')
}

sortLargeFile('generatedFile1.txt');