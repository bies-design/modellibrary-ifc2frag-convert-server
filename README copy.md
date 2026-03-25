# modellibrary-tus-server

## start by docker compose
1. 創建.env，可以參考env.example
2. 背景啟動
```bash
docker compose up -d  
```
3. 關閉並移除容器
```bash
docker compose down 
```

-----------
#### 編譯可使用的環境<br/>
```bash
docker compose build
```
#### 拉下或上傳image(基本上不需要手動處理此) <br/>
```bash
docker pull IMAGENAME:TAG
docker push IMAGENAME:TAG
```

## start by node (npm)
> 必須要先有對應環境，建議可以用 nvm 安裝管理<br/>
```bash
// staging
npm run update
npm run dev

// demo or release
npm run update
npm run create
npm run prod
```