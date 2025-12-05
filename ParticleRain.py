import pygame
import random
import math

# 初始化Pygame
pygame.init()

# 屏幕设置
WIDTH, HEIGHT = 800, 600
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("霓虹光影粒子雨")
clock = pygame.time.Clock()

# 颜色定义
BLACK = (0, 0, 0)
NEON_COLORS = [
    (255, 0, 255),  # 紫色
    (0, 255, 255),  # 青色
    (255, 255, 0),  # 黄色
    (255, 0, 0),  # 红色
    (0, 255, 0),  # 绿色
    (0, 0, 255),  # 蓝色
]


class Particle:
    def __init__(self):
        self.reset()
        self.trail = []  # 轨迹点列表，用于创建拖尾效果

    def reset(self):
        """重置粒子状态"""
        self.x = random.randint(0, WIDTH)
        self.y = random.randint(-100, -10)
        self.speed = random.uniform(2, 8)
        self.size = random.uniform(2, 6)
        self.color = random.choice(NEON_COLORS)
        self.glow_intensity = random.uniform(0.7, 1.0)
        self.state = "falling"  # falling, stopped, falling_again
        self.stop_timer = 0
        self.stop_duration = random.uniform(30, 120)  # 停留帧数
        self.trail = []

    def update(self):
        """更新粒子状态"""
        if self.state == "falling":
            self.y += self.speed
            # 添加当前位罤到轨迹
            self.trail.append((self.x, self.y))
            if len(self.trail) > 10:  # 限制轨迹长度
                self.trail.pop(0)

            # 检查是否到达停留点
            if self.y >= random.randint(HEIGHT // 3, HEIGHT * 2 // 3):
                self.state = "stopped"
                self.stop_timer = 0

        elif self.state == "stopped":
            self.stop_timer += 1
            if self.stop_timer >= self.stop_duration:
                self.state = "falling_again"

        elif self.state == "falling_again":
            self.y += self.speed
            self.trail.append((self.x, self.y))
            if len(self.trail) > 10:
                self.trail.pop(0)

            # 重置粒子当它离开屏幕底部
            if self.y > HEIGHT + 20:
                self.reset()

    def draw(self, surface):
        """绘制粒子及其光影效果"""
        if self.state == "stopped":
            # 停止时绘制更亮的光效
            self.draw_glow(surface, intensity=1.5)
        else:
            # 下落时绘制正常光效
            self.draw_glow(surface, intensity=self.glow_intensity)

        # 绘制轨迹拖尾效果
        for i, (trail_x, trail_y) in enumerate(self.trail):
            alpha = int(255 * (i / len(self.trail)) * 0.3)
            if alpha > 0:
                trail_size = max(1, self.size * (i / len(self.trail)))
                glow_color = (*self.color, alpha)
                self.draw_soft_circle(surface, trail_x, trail_y, trail_size, glow_color)

    def draw_glow(self, surface, intensity=1.0):
        """绘制霓虹光影效果"""
        # 创建多个不同大小的半透明圆形来模拟光晕效果
        for i in range(3):
            glow_size = self.size * (3 - i) * intensity
            alpha = int(100 * (0.3 ** i) * intensity)
            if alpha > 0:
                glow_color = (*self.color, alpha)
                self.draw_soft_circle(surface, self.x, self.y, glow_size, glow_color)

        # 绘制核心粒子
        core_color = (*self.color, 255)
        self.draw_soft_circle(surface, self.x, self.y, self.size, core_color)

    def draw_soft_circle(self, surface, x, y, radius, color):
        """绘制软边缘圆形"""
        if radius <= 0:
            return

        # 创建临时表面
        try:
            temp_surface = pygame.Surface((int(radius * 2) * 2, int(radius * 2) * 2), pygame.SRCALPHA)
            pygame.draw.circle(temp_surface, color, (int(radius * 2), int(radius * 2)), int(radius))
            surface.blit(temp_surface, (x - radius * 2, y - radius * 2))
        except:
            # 如果surface创建失败，绘制简单圆形
            if len(color) == 4:  # 有alpha通道
                s = pygame.Surface((int(radius * 2), int(radius * 2)), pygame.SRCALPHA)
                pygame.draw.circle(s, color, (int(radius), int(radius)), int(radius))
                surface.blit(s, (int(x - radius), int(y - radius)))
            else:
                pygame.draw.circle(surface, color, (int(x), int(y)), int(radius))


# 创建粒子列表
particles = [Particle() for _ in range(200)]

# 主循环
running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_ESCAPE:
                running = False

    # 更新粒子
    for particle in particles:
        particle.update()

    # 绘制
    screen.fill(BLACK)

    # 绘制所有粒子
    for particle in particles:
        particle.draw(screen)

    pygame.display.flip()
    clock.tick(60)

pygame.quit()
