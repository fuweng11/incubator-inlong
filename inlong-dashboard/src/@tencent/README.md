# Tencent 内部 WeData 版本

## About

* @tencent 下为内部特有的 UI，依赖需在内部目录下单独安装

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
