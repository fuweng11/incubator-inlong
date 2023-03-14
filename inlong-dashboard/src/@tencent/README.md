# Tencent 内部 WeData 版本

## About

* @tencent 下为内部特有的 UI，不要在外部使用，后续规划会以一个单独的模块的形式

## Dev

1. 在 `src` 目录下新建 `.env.local` 配置文件，在文件内声明配置 `INLONG_ENV=tencent` (`.env.local` 默认不提交到 git 版本库，而 `.env` 是会提交，我们不建议使用 `.env` 声明 INLONG_ENV，这会影响其他 INLONG 版本的环境，必要时，应在 CI 工具中去配置不同的变量)
2. 执行 `npm run dev`

## Build

* 若本地已配置 `.env.local`，可直接在本地执行 `npm run build`
* 若使用 CI:

  * 由于 `.env.local` 默认不提交到 git 版本库，因此可在 CI 中自动生成一个配置文件
  * 直接配置系统环境变量:

  ```sh
      export INLONG_ENV=tencent
      unset INLONG_ENV
  ```
* 使用docker:

  * 本地构建镜像，请在 `.env.local`添加环境变量

  ```sh
      PUBLIC_URL=/inlong
      INLONG_ENV=tencent
  ```
  * 打包构建、推送镜像的shell脚本:

  ```sh
      echo "打包代码"
      npm run build

      echo "制作并推送docker镜像"
      tag=`date '+%Y%m%d_%H%M%S'`
      echo "TAG=${tag}"
      docker build -f src/@tencent/Dockerfile -t mirrors.tencent.com/wedata-oa/inlong-dev:$tag .
      docker push mirrors.tencent.com/wedata-oa/inlong-dev:$tag
  ```
