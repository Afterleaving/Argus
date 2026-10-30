# AGENTS.md

本文件为 Codex（Codex.ai/code）在本仓库中处理代码时提供指导。

## 构建命令

```bash
# 设置 JDK 21（必需 —— record 语法、虚拟线程）
export JAVA_HOME="D:\Develop\DevelopTool\StudyEnvironment\PhpWebStudy-Data\app\openjdk-21.0.9"

# 编译
cd Argus-backend && ./mvnw clean compile

# 运行测试
./mvnw test

# 运行单个测试
./mvnw test -Dtest=ClassName#methodName

# 打包
./mvnw clean package -DskipTests

# 启动应用（默认使用 local profile）
./mvnw spring-boot:run
```

## 技术栈

- **Java 21** + Spring Boot 3.5.0，Spring MVC（不使用 WebFlux）
- **MyBatis-Plus 3.5.15**（不使用 JdbcTemplate）处理所有数据库访问
- **PostgreSQL** 作为数据库（驱动已在 pom 中）
- **Maven**，使用 wrapper（`mvnw`）
- **Lombok 1.18.34**（已引入但当前未使用 —— 实体的 getter/setter 为手写）
- **JJWT 0.12.6** 用于 JWT 访问令牌
- **Spring Security Crypto**（BCrypt）用于密码哈希
- **Knife4j 4.5.0** 用于 API 文档

## 架构

```
com.argus.rag
├── ArgusBackendApplication        # @SpringBootApplication 入口
├── common/                        # 共享基础设施
│   ├── api/ApiResponse            # 统一响应 record：{success, data, message}
│   ├── enums/                     # SystemRole、UserStatus、GroupRole 等
│   └── exception/                 # BusinessException(400)、ForbiddenException(403)、
│                                  # UnauthorizedException(401)、GlobalExceptionHandler
├── auth/                          # 认证与授权
│   ├── config/                    # AuthProperties（@ConfigurationProperties，前缀 argus.rag.auth）、
│   │                              # AuthConfiguration（PasswordHasher bean、Clock bean）、
│   │                              # DevAdminInitializer（@Profile("dev") 初始化管理员账号）
│   ├── controller/AuthController  # /api/auth/{login,register,refresh,logout,me}
│   ├── service/                   # AuthService（登录/注册/刷新逻辑）、
│   │                              # PasswordHasher 接口、RefreshTokenRecord
│   ├── security/                  # JwtAuthenticationFilter（OncePerRequestFilter）、
│   │                              # JwtAccessTokenService（签发/解析 JWT）、
│   │                              # RefreshTokenService（refresh token 增删改查）、
│   │                              # AuthCookieSupport（refresh token cookie）
│   ├── model/entity/UserRefreshToken  # MyBatis-Plus 实体 → user_refresh_tokens 表
│   ├── mapper/UserRefreshTokenMapper  # MyBatis-Plus BaseMapper
│   └── CurrentUserService         # 从请求属性中提取已认证用户
└── user/                          # 用户管理
    ├── controller/                # AccountController（/api/account/change-password）、
    │                              # AdminUserController（/api/admin/users 增删改查）
    ├── service/                   # AccountService、AdminUserService、UserQueryService
    ├── model/entity/User          # MyBatis-Plus 实体 → users 表
    ├── model/dto/                 # 请求 record（ChangePassword、UpdateUserStatus 等）
    ├── model/vo/                  # 响应 record（AdminUserItemResponse）
    └── mapper/UserMapper          # MyBatis-Plus BaseMapper + 自定义 XML 方法
```

## 关键约定

### MyBatis-Plus 模式
- 所有数据库访问使用 `BaseMapper` + `LambdaQueryWrapper`，绝不使用 `JdbcTemplate`
- 实体不继承任何类，但使用 `@TableName` / `@TableId(type = IdType.AUTO)` 注解
- 带 `FOR UPDATE` 行锁的复杂查询放在 `src/main/resources/mappers/*.xml` 中
- 枚举使用 `default-enum-type-handler: org.apache.ibatis.type.EnumTypeHandler`（以 `name()` 字符串形式存储）

### API 响应模式
- 每个 controller 方法都返回 `ApiResponse<T>` —— 一个包含 `success()`、`data()`、`message()` 的 `record`
- 绝不抛出原始异常；应抛出 `BusinessException`/`ForbiddenException`/`UnauthorizedException`，由 `GlobalExceptionHandler` 映射为相应的 HTTP 状态码

### 认证流程
- `JwtAuthenticationFilter` 提取 Bearer token → 将 `AuthenticatedUser` 设置为请求属性
- `CurrentUserService` 读取该属性，提供 `CurrentUser` record
- Controller 调用 `currentUserService.getRequiredCurrentUser(request)` 或 `requireSystemAdmin(request)`
- Refresh token 存储于 `user_refresh_tokens` 表，通过 httpOnly cookie 发送

## 配置

- **激活的 profile**：`local`（`application.yml` 中的默认值）
- **认证配置前缀**：`argus.rag.auth`（映射到 `AuthProperties` 类）
- **MyBatis-Plus XML mapper 位置**：`classpath*:/mappers/**/*.xml`
- **枚举处理**：使用枚举的 `.name()` 值以 VARCHAR 形式存储
